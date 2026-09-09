/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.extensions.channel.wecom.kf;

import io.agentscope.core.message.Msg;
import io.agentscope.extensions.channel.common.BotLoopGuard;
import io.agentscope.extensions.channel.common.IdempotencyStore;
import io.agentscope.harness.agent.gateway.Gateway;
import io.agentscope.harness.agent.gateway.channel.Channel;
import io.agentscope.harness.agent.gateway.channel.ChannelConfig;
import io.agentscope.harness.agent.gateway.channel.ChannelRouter;
import io.agentscope.harness.agent.gateway.channel.InboundMessage;
import io.agentscope.harness.agent.gateway.channel.OutboundAddress;
import io.agentscope.harness.agent.gateway.channel.RouteResult;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import me.chanjar.weixin.cp.bean.kf.WxCpKfMsgListResp.WxCpKfMsgItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

/**
 * WeCom Customer Service (微信客服) channel adapter.
 *
 * <p>Unlike a normal WeCom application callback, a Customer Service callback is only a sync
 * notification. Callback tokens are serialized through an internal queue, messages are fetched via
 * WxJava sync_msg, customer records are normalized into {@link InboundMessage}, and replies are
 * sent via send_msg.
 */
public final class WeComKfChannel implements Channel {

    private static final Logger log = LoggerFactory.getLogger(WeComKfChannel.class);

    /** type value used by ChannelFactory registrations and agentscope.json. */
    public static final String TYPE = "wecom-kf";

    private final String channelId;
    private final ChannelConfig config;
    private final WeComKfChannelProperties properties;
    private final WeComKfWxClient wxClient;
    private final WeComKfOutboundClient outboundClient;
    private final WeComKfInboundMapper mapper;
    private final IdempotencyStore idempotency;
    private final BotLoopGuard botLoopGuard;
    private final ChannelRouter router;
    private final WeComKfChannelRegistry registry;
    private final WeComKfCursorStore cursorStore;

    private volatile Gateway gateway;
    private volatile Sinks.Many<WeComKfWxClient.CallbackSignal> callbackSink;
    private volatile Disposable callbackSubscription;

    private WeComKfChannel(
            String channelId,
            ChannelConfig config,
            WeComKfChannelProperties properties,
            WeComKfWxClient wxClient,
            WeComKfOutboundClient outboundClient,
            WeComKfInboundMapper mapper,
            IdempotencyStore idempotency,
            BotLoopGuard botLoopGuard,
            ChannelRouter router,
            WeComKfChannelRegistry registry,
            WeComKfCursorStore cursorStore) {
        this.channelId = Objects.requireNonNull(channelId, "channelId");
        this.config = Objects.requireNonNull(config, "config");
        this.properties = Objects.requireNonNull(properties, "properties");
        this.wxClient = Objects.requireNonNull(wxClient, "wxClient");
        this.outboundClient = Objects.requireNonNull(outboundClient, "outboundClient");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.idempotency = Objects.requireNonNull(idempotency, "idempotency");
        this.botLoopGuard = Objects.requireNonNull(botLoopGuard, "botLoopGuard");
        this.router = Objects.requireNonNull(router, "router");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.cursorStore = Objects.requireNonNull(cursorStore, "cursorStore");
    }

    /** Factory matching the other AgentScope channel extensions. */
    public static WeComKfChannel fromProperties(
            String channelId, ChannelConfig routing, Map<String, Object> rawProperties) {
        return fromProperties(
                channelId, routing, rawProperties, new InMemoryWeComKfCursorStore());
    }

    /**
     * Factory with an application-provided cursor store.
     *
     * <p>Use {@link RedissonWeComKfCursorStore} for multi-Pod deployments so callbacks handled by
     * different Pods share the same {@code sync_msg} cursor and synchronization lease.
     */
    public static WeComKfChannel fromProperties(
            String channelId,
            ChannelConfig routing,
            Map<String, Object> rawProperties,
            WeComKfCursorStore cursorStore) {
        WeComKfChannelProperties props =
                WeComKfChannelProperties.from(channelId, rawProperties);
        WeComKfWxClient wxClient = new WeComKfWxClient(props);
        return new WeComKfChannel(
                channelId,
                routing,
                props,
                wxClient,
                new WeComKfOutboundClient(wxClient, props.openKfid()),
                new WeComKfInboundMapper(channelId, props.openKfid()),
                new IdempotencyStore(),
                new BotLoopGuard(),
                new ChannelRouter(routing.defaultAgentId()),
                WeComKfChannelRegistry.instance(),
                cursorStore);
    }

    @Override
    public String channelId() {
        return channelId;
    }

    @Override
    public ChannelConfig config() {
        return config;
    }

    @Override
    public void init(Gateway gateway) {
        if (this.gateway == null) {
            this.gateway = Objects.requireNonNull(gateway, "gateway");
        }
    }

    @Override
    public synchronized void start() {
        if (callbackSubscription != null && !callbackSubscription.isDisposed()) {
            return;
        }
        Sinks.Many<WeComKfWxClient.CallbackSignal> sink =
                Sinks.many().unicast().onBackpressureBuffer();
        Disposable subscription =
                sink.asFlux()
                        .concatMap(
                                signal ->
                                        synchronize(signal)
                                                .onErrorResume(
                                                        error -> {
                                                            log.warn(
                                                                    "WeCom KF sync failed for channel '{}': {}",
                                                                    channelId,
                                                                    error.getMessage());
                                                            return Mono.empty();
                                                        }))
                        .subscribe();
        callbackSink = sink;
        callbackSubscription = subscription;
        registry.register(this);
        log.info(
                "WeCom KF channel '{}' started: corpId={}, openKfid={}, callbackPath={}",
                channelId,
                properties.corpId(),
                properties.openKfid(),
                properties.callbackPath());
    }

    @Override
    public synchronized void stop() {
        registry.unregister(channelId);
        Sinks.Many<WeComKfWxClient.CallbackSignal> sink = callbackSink;
        callbackSink = null;
        if (sink != null) {
            sink.tryEmitComplete();
        }
        Disposable subscription = callbackSubscription;
        callbackSubscription = null;
        if (subscription != null) {
            subscription.dispose();
        }
        log.info("WeCom KF channel '{}' stopped", channelId);
    }

    @Override
    public Mono<Msg> dispatch(InboundMessage message) {
        Objects.requireNonNull(message, "message");
        Gateway currentGateway = gateway;
        if (currentGateway == null) {
            return Mono.error(
                    new IllegalStateException(
                            "WeComKfChannel '" + channelId + "' has no gateway"));
        }
        RouteResult route = router.resolveRoute(config, message);
        return currentGateway
                .run(
                        route.context(),
                        message.messages(),
                        route.outboundAddress(),
                        message.runtimeContext(),
                        message)
                .flatMap(reply -> sendReply(route.outboundAddress(), reply).thenReturn(reply));
    }

    @Override
    public void deliver(OutboundAddress address, List<Msg> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        outboundClient
                .send(address, messages)
                .doOnError(
                        error ->
                                log.warn(
                                        "WeCom KF channel '{}' deliver failed: {}",
                                        channelId,
                                        error.getMessage()))
                .subscribe();
    }

    synchronized boolean enqueueCallback(WeComKfWxClient.CallbackSignal signal) {
        Sinks.Many<WeComKfWxClient.CallbackSignal> sink = callbackSink;
        if (sink == null) {
            return false;
        }
        return sink.tryEmitNext(signal) == Sinks.EmitResult.OK;
    }

    WeComKfWxClient wxClient() {
        return wxClient;
    }

    WeComKfChannelProperties properties() {
        return properties;
    }

    private Mono<Void> synchronize(WeComKfWxClient.CallbackSignal signal) {
        return Mono.usingWhen(
                cursorStore.acquire(channelId, properties.openKfid()),
                lease ->
                        Mono.fromCallable(
                                        () ->
                                                wxClient.syncAll(
                                                        lease.cursor(),
                                                        signal.token(),
                                                        properties.openKfid(),
                                                        properties.syncLimit(),
                                                        properties.voiceFormat()))
                                .subscribeOn(Schedulers.boundedElastic())
                                .flatMap(
                                        batch ->
                                                Flux.fromIterable(batch.messages())
                                                        .concatMap(this::handleItem)
                                                        .then(lease.commit(batch.nextCursor()))),
                WeComKfCursorStore.CursorLease::release,
                (lease, error) -> lease.release(),
                WeComKfCursorStore.CursorLease::release);
    }

    private Mono<Void> handleItem(WxCpKfMsgItem item) {
        String msgId = item.getMsgId();
        if (msgId != null
                && !msgId.isBlank()
                && !idempotency.firstSeen(properties.openKfid() + "|" + msgId)) {
            return Mono.empty();
        }

        Optional<InboundMessage> inbound = mapper.map(item);
        if (inbound.isEmpty()) {
            return Mono.empty();
        }
        InboundMessage message = inbound.get();
        if (!botLoopGuard.allow(message.peer().key())) {
            log.warn(
                    "WeCom KF bot-loop guard tripped for peer='{}' (channelId='{}')",
                    message.peer().key(),
                    channelId);
            return Mono.empty();
        }
        return dispatch(message)
                .then()
                .onErrorResume(
                        error -> {
                            log.warn(
                                    "WeCom KF agent run failed for msgId={} (channelId='{}'): {}",
                                    msgId,
                                    channelId,
                                    error.getMessage());
                            return Mono.empty();
                        });
    }

    private Mono<Void> sendReply(OutboundAddress address, Msg reply) {
        if (reply == null) {
            return Mono.empty();
        }
        return outboundClient
                .send(address, List.of(reply))
                .doOnError(
                        error ->
                                log.warn(
                                        "WeCom KF channel '{}' reply send failed: {}",
                                        channelId,
                                        error.getMessage()));
    }
}
