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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.agentscope.harness.agent.gateway.channel.InboundMessage;
import io.agentscope.harness.agent.gateway.channel.PeerKind;
import java.util.Optional;
import me.chanjar.weixin.cp.bean.kf.WxCpKfMsgListResp.WxCpKfMsgItem;
import me.chanjar.weixin.cp.bean.kf.msg.WxCpKfTextMsg;
import org.junit.jupiter.api.Test;

class WeComKfInboundMapperTest {

    private final WeComKfInboundMapper mapper = new WeComKfInboundMapper("kf-main", "wk-main");

    @Test
    void shouldMapCustomerTextIntoDirectInboundMessage() {
        Optional<InboundMessage> mapped = mapper.map(textItem(3, "wk-main", "external-1", "hello"));

        assertTrue(mapped.isPresent());
        InboundMessage inbound = mapped.orElseThrow();
        assertEquals("kf-main", inbound.channelId());
        assertEquals("wk-main", inbound.accountId());
        assertEquals(PeerKind.DIRECT, inbound.peer().kind());
        assertEquals("external-1", inbound.peer().id());
        assertEquals("external-1", inbound.senderId());
        assertEquals("hello", inbound.messages().get(0).getTextContent());
    }

    @Test
    void shouldIgnoreServicerAndWrongAccountMessages() {
        assertTrue(mapper.map(textItem(5, "wk-main", "external-1", "staff reply")).isEmpty());
        assertTrue(mapper.map(textItem(3, "wk-other", "external-1", "hello")).isEmpty());
    }

    @Test
    void shouldIgnoreUnsupportedMessageType() {
        WxCpKfMsgItem item = textItem(3, "wk-main", "external-1", "hello");
        item.setMsgType("image");
        assertTrue(mapper.map(item).isEmpty());
    }

    private static WxCpKfMsgItem textItem(
            int origin, String openKfid, String externalUserId, String content) {
        WxCpKfTextMsg text = new WxCpKfTextMsg();
        text.setContent(content);

        WxCpKfMsgItem item = new WxCpKfMsgItem();
        item.setMsgId("msg-1");
        item.setOrigin(origin);
        item.setOpenKfid(openKfid);
        item.setExternalUserId(externalUserId);
        item.setMsgType("text");
        item.setText(text);
        return item;
    }
}
