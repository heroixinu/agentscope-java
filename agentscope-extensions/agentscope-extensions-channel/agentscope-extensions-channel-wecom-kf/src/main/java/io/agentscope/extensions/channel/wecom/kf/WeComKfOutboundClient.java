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
import io.agentscope.harness.agent.gateway.channel.OutboundAddress;
import io.agentscope.harness.agent.gateway.channel.PeerKind;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Sends AgentScope replies through WxJava's WeCom Customer Service send_msg API. */
final class WeComKfOutboundClient {

    private final WeComKfWxClient wxClient;
    private final String openKfid;

    WeComKfOutboundClient(WeComKfWxClient wxClient, String openKfid) {
        this.wxClient = wxClient;
        this.openKfid = openKfid;
    }

    Mono<Void> send(OutboundAddress address, List<Msg> messages) {
        if (messages == null || messages.isEmpty()) {
            return Mono.empty();
        }
        PeerTarget target = parseAddress(address);
        if (target.kind() != PeerKind.DIRECT) {
            return Mono.error(
                    new IllegalArgumentException(
                            "WeCom KF outbound only supports DIRECT peers, got " + target.kind()));
        }
        if (target.id() == null || target.id().isBlank()) {
            return Mono.error(new IllegalArgumentException("WeCom KF recipient is empty"));
        }
        return Flux.fromIterable(messages).concatMap(msg -> sendOne(target.id(), msg)).then();
    }

    private Mono<Void> sendOne(String externalUserId, Msg msg) {
        String text = msg.getTextContent();
        if (text == null || text.isBlank()) {
            return Mono.empty();
        }
        return Mono.fromCallable(
                        () -> {
                            wxClient.sendText(externalUserId, openKfid, text);
                            return Boolean.TRUE;
                        })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    private static PeerTarget parseAddress(OutboundAddress address) {
        if (address == null || address.to() == null || address.to().isBlank()) {
            throw new IllegalArgumentException("WeCom KF outbound address is empty");
        }
        String to = address.to();
        int first = to.indexOf(':');
        if (first < 0) {
            return new PeerTarget(PeerKind.DIRECT, to);
        }
        String rest = to.substring(first + 1);
        int second = rest.indexOf(':');
        String kindRaw = second < 0 ? "DIRECT" : rest.substring(0, second);
        String id = second < 0 ? rest : rest.substring(second + 1);
        PeerKind kind;
        try {
            kind = PeerKind.valueOf(kindRaw.toUpperCase());
        } catch (IllegalArgumentException e) {
            kind = PeerKind.DIRECT;
        }
        return new PeerTarget(kind, id);
    }

    private record PeerTarget(PeerKind kind, String id) {}
}
