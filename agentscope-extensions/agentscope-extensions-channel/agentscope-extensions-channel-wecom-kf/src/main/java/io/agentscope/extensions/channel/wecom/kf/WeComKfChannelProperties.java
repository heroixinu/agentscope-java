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

import java.util.Map;
import java.util.Objects;

/** Provider-specific configuration for one WeCom Customer Service (微信客服) account. */
public record WeComKfChannelProperties(
        String corpId,
        String secret,
        String token,
        String encodingAesKey,
        String openKfid,
        String callbackPath,
        String apiBase,
        int syncLimit,
        int voiceFormat) {

    private static final String DEFAULT_API_BASE = "https://qyapi.weixin.qq.com";

    public WeComKfChannelProperties {
        requireText(corpId, "wecom-kf.corpId is required");
        requireText(secret, "wecom-kf.secret is required");
        requireText(token, "wecom-kf.token is required");
        requireText(openKfid, "wecom-kf.openKfid is required");
        if (encodingAesKey == null || encodingAesKey.length() != 43) {
            throw new IllegalArgumentException(
                    "wecom-kf.encodingAesKey must be exactly 43 characters");
        }
        if (callbackPath == null || callbackPath.isBlank()) {
            throw new IllegalArgumentException("wecom-kf.callbackPath is required");
        }
        if (apiBase == null || apiBase.isBlank()) {
            apiBase = DEFAULT_API_BASE;
        }
        if (syncLimit < 1 || syncLimit > 1000) {
            throw new IllegalArgumentException("wecom-kf.syncLimit must be between 1 and 1000");
        }
        if (voiceFormat != 0 && voiceFormat != 1) {
            throw new IllegalArgumentException("wecom-kf.voiceFormat must be 0 (AMR) or 1 (SILK)");
        }
    }

    /** Reads channel-specific properties from an agentscope.json properties block. */
    public static WeComKfChannelProperties from(
            String channelId, Map<String, Object> rawProperties) {
        Objects.requireNonNull(channelId, "channelId");
        Map<String, Object> p = rawProperties != null ? rawProperties : Map.of();
        String expectedCallbackPath = defaultCallbackPath(channelId);
        String callbackPath = asStringOr(p, "callbackPath", expectedCallbackPath);
        if (!expectedCallbackPath.equals(callbackPath)) {
            throw new IllegalArgumentException(
                    "wecom-kf.callbackPath is fixed by the Spring controller and must be '"
                            + expectedCallbackPath
                            + "'");
        }
        return new WeComKfChannelProperties(
                asString(p, "corpId"),
                asString(p, "secret"),
                asString(p, "token"),
                asString(p, "encodingAesKey"),
                asString(p, "openKfid"),
                callbackPath,
                asStringOr(p, "apiBase", DEFAULT_API_BASE),
                asIntOr(p, "syncLimit", 1000),
                asIntOr(p, "voiceFormat", 0));
    }

    public static String defaultCallbackPath(String channelId) {
        Objects.requireNonNull(channelId, "channelId");
        return "/api/channels/wecom-kf/" + channelId + "/callback";
    }

    private static void requireText(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(message);
        }
    }

    private static String asString(Map<String, Object> p, String key) {
        Object value = p.get(key);
        return value == null ? null : value.toString();
    }

    private static String asStringOr(Map<String, Object> p, String key, String fallback) {
        String value = asString(p, key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int asIntOr(Map<String, Object> p, String key, int fallback) {
        Object value = p.get(key);
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "wecom-kf." + key + " must be an integer, got: " + value, e);
        }
    }
}
