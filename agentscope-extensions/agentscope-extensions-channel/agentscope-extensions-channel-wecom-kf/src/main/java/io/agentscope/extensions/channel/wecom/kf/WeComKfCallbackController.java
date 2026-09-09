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

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * WeCom Customer Service callback endpoint.
 *
 * <p>The POST callback is a notification only. It contains a short-lived Token and OpenKfId; the
 * actual customer messages must be fetched through sync_msg. This controller therefore validates
 * and decrypts with WxJava, enqueues the signal on the target channel and returns immediately.
 */
@RestController
@RequestMapping("/api/channels/wecom-kf")
public class WeComKfCallbackController {

    private static final Logger log = LoggerFactory.getLogger(WeComKfCallbackController.class);

    private final WeComKfChannelRegistry registry;

    public WeComKfCallbackController() {
        this(WeComKfChannelRegistry.instance());
    }

    WeComKfCallbackController(WeComKfChannelRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    @GetMapping(value = "/{channelId}/callback", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> verify(
            @PathVariable String channelId,
            @RequestParam("msg_signature") String signature,
            @RequestParam("timestamp") String timestamp,
            @RequestParam("nonce") String nonce,
            @RequestParam("echostr") String echoStr) {
        WeComKfChannel channel = registry.get(channelId);
        if (channel == null) {
            return ResponseEntity.notFound().build();
        }
        try {
            String plain = channel.wxClient().decryptEcho(signature, timestamp, nonce, echoStr);
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(plain);
        } catch (RuntimeException e) {
            log.warn(
                    "WeCom KF callback verification failed for channel '{}': {}",
                    channelId,
                    e.getMessage());
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
    }

    @PostMapping(value = "/{channelId}/callback", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> callback(
            @PathVariable String channelId,
            @RequestParam("msg_signature") String signature,
            @RequestParam("timestamp") String timestamp,
            @RequestParam("nonce") String nonce,
            @RequestBody String body) {
        WeComKfChannel entryChannel = registry.get(channelId);
        if (entryChannel == null) {
            return ResponseEntity.notFound().build();
        }

        try {
            WeComKfWxClient.CallbackSignal signal =
                    entryChannel.wxClient().parseCallback(body, signature, timestamp, nonce);
            WeComKfChannel target = entryChannel;
            if (signal.openKfid() != null && !signal.openKfid().isBlank()) {
                WeComKfChannel routed = registry.getByOpenKfid(signal.openKfid());
                if (routed != null) {
                    target = routed;
                } else if (!entryChannel.properties().openKfid().equals(signal.openKfid())) {
                    log.debug(
                            "Ignoring WeCom KF callback for unconfigured openKfid={}",
                            signal.openKfid());
                    return ResponseEntity.ok("success");
                }
            }

            if (!target.enqueueCallback(signal)) {
                log.warn("WeCom KF channel '{}' is not accepting callbacks", target.channelId());
            }
            return ResponseEntity.ok("success");
        } catch (RuntimeException e) {
            log.warn(
                    "Invalid WeCom KF callback for channel '{}': {}", channelId, e.getMessage());
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("");
        }
    }
}
