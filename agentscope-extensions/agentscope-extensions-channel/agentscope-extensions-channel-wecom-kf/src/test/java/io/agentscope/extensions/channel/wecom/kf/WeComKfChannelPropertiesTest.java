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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WeComKfChannelPropertiesTest {

    @Test
    void shouldApplyDefaultsAndFixedCallbackPath() {
        WeComKfChannelProperties properties =
                WeComKfChannelProperties.from("support", requiredProperties());

        assertEquals("/api/channels/wecom-kf/support/callback", properties.callbackPath());
        assertEquals("https://qyapi.weixin.qq.com", properties.apiBase());
        assertEquals(1000, properties.syncLimit());
        assertEquals(0, properties.voiceFormat());
    }

    @Test
    void shouldRejectCallbackPathThatControllerCannotExpose() {
        Map<String, Object> properties = new HashMap<>(requiredProperties());
        properties.put("callbackPath", "/custom/wecom/callback");

        assertThrows(
                IllegalArgumentException.class,
                () -> WeComKfChannelProperties.from("support", properties));
    }

    @Test
    void shouldValidateSyncLimitAndVoiceFormat() {
        Map<String, Object> properties = new HashMap<>(requiredProperties());
        properties.put("syncLimit", 1001);
        assertThrows(
                IllegalArgumentException.class,
                () -> WeComKfChannelProperties.from("support", properties));

        properties = new HashMap<>(requiredProperties());
        properties.put("voiceFormat", 2);
        Map<String, Object> invalidVoice = properties;
        assertThrows(
                IllegalArgumentException.class,
                () -> WeComKfChannelProperties.from("support", invalidVoice));
    }

    private static Map<String, Object> requiredProperties() {
        return Map.of(
                "corpId", "ww-test",
                "secret", "secret",
                "token", "token",
                "encodingAesKey", "a".repeat(43),
                "openKfid", "wk-test");
    }
}
