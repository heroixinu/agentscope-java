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
import io.agentscope.core.message.MsgRole;
import io.agentscope.harness.agent.gateway.channel.InboundMessage;
import io.agentscope.harness.agent.gateway.channel.Peer;
import io.agentscope.harness.agent.gateway.channel.PeerKind;
import java.util.List;
import java.util.Optional;
import me.chanjar.weixin.cp.bean.kf.WxCpKfMsgListResp.WxCpKfMsgItem;

/** Maps WxJava Customer Service sync_msg records into AgentScope inbound messages. */
public final class WeComKfInboundMapper {

    private static final int ORIGIN_CUSTOMER = 3;

    private final String channelId;
    private final String openKfid;

    public WeComKfInboundMapper(String channelId, String openKfid) {
        this.channelId = channelId;
        this.openKfid = openKfid;
    }

    /**
     * Only actual customer text messages are dispatched. Servicer messages (origin=5) and system
     * events (origin=4) must never be fed back into the agent or they can create reply loops.
     */
    public Optional<InboundMessage> map(WxCpKfMsgItem item) {
        if (item == null || !Integer.valueOf(ORIGIN_CUSTOMER).equals(item.getOrigin())) {
            return Optional.empty();
        }
        if (!openKfid.equals(item.getOpenKfid())) {
            return Optional.empty();
        }
        if (!"text".equalsIgnoreCase(item.getMsgType()) || item.getText() == null) {
            return Optional.empty();
        }
        String externalUserId = item.getExternalUserId();
        String content = item.getText().getContent();
        if (externalUserId == null
                || externalUserId.isBlank()
                || content == null
                || content.isBlank()) {
            return Optional.empty();
        }

        Msg msg =
                Msg.builder()
                        .role(MsgRole.USER)
                        .name(externalUserId)
                        .textContent(content)
                        .build();
        Peer peer = new Peer(PeerKind.DIRECT, externalUserId);
        return Optional.of(
                InboundMessage.builder(channelId, peer, List.of(msg))
                        .accountId(openKfid)
                        .senderId(externalUserId)
                        .build());
    }
}
