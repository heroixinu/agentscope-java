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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import me.chanjar.weixin.common.error.WxErrorException;
import me.chanjar.weixin.cp.api.WxCpKfService;
import me.chanjar.weixin.cp.api.WxCpService;
import me.chanjar.weixin.cp.api.impl.WxCpServiceImpl;
import me.chanjar.weixin.cp.bean.kf.WxCpKfMsgListResp;
import me.chanjar.weixin.cp.bean.kf.WxCpKfMsgListResp.WxCpKfMsgItem;
import me.chanjar.weixin.cp.bean.kf.WxCpKfMsgSendRequest;
import me.chanjar.weixin.cp.bean.kf.msg.WxCpKfTextMsg;
import me.chanjar.weixin.cp.bean.message.WxCpXmlMessage;
import me.chanjar.weixin.cp.config.impl.WxCpDefaultConfigImpl;
import me.chanjar.weixin.cp.util.crypto.WxCpCryptUtil;

/**
 * Thin WxJava facade used by the channel. It deliberately owns all WeCom credentials, callback
 * crypto, access-token handling and Customer Service API calls so protocol details do not leak into
 * the AgentScope routing layer.
 */
final class WeComKfWxClient {

    private final WxCpDefaultConfigImpl configStorage;
    private final WxCpKfService kfService;
    private final WxCpCryptUtil cryptUtil;

    WeComKfWxClient(WeComKfChannelProperties properties) {
        Objects.requireNonNull(properties, "properties");
        WxCpDefaultConfigImpl storage = new WxCpDefaultConfigImpl();
        storage.setCorpId(properties.corpId());
        storage.setCorpSecret(properties.secret());
        storage.setToken(properties.token());
        storage.setAesKey(properties.encodingAesKey());
        storage.setBaseApiUrl(properties.apiBase());

        WxCpService service = new WxCpServiceImpl();
        service.setWxCpConfigStorage(storage);

        this.configStorage = storage;
        this.kfService = service.getKfService();
        this.cryptUtil = new WxCpCryptUtil(storage);
    }

    String decryptEcho(String signature, String timestamp, String nonce, String echoStr) {
        return cryptUtil.decryptContent(signature, timestamp, nonce, echoStr);
    }

    CallbackSignal parseCallback(
            String body, String signature, String timestamp, String nonce) {
        WxCpXmlMessage callback =
                WxCpXmlMessage.fromEncryptedXml(
                        body, configStorage, timestamp, nonce, signature);
        String token = callback.getToken();
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("WeCom KF callback does not contain Token");
        }
        return new CallbackSignal(token, callback.getOpenKfId());
    }

    SyncBatch syncAll(
            String cursor,
            String token,
            String openKfid,
            int limit,
            int voiceFormat)
            throws WxErrorException {
        List<WxCpKfMsgItem> messages = new ArrayList<>();
        String currentCursor = normalizeCursor(cursor);

        while (true) {
            String requestCursor = currentCursor;
            WxCpKfMsgListResp response =
                    kfService.syncMsg(requestCursor, token, limit, voiceFormat, openKfid);
            if (response.getMsgList() != null) {
                messages.addAll(response.getMsgList());
            }

            String nextCursor = normalizeCursor(response.getNextCursor());
            boolean hasMore = Integer.valueOf(1).equals(response.getHasMore());
            if (nextCursor != null) {
                currentCursor = nextCursor;
            }
            if (!hasMore) {
                break;
            }
            if (nextCursor == null || Objects.equals(nextCursor, requestCursor)) {
                throw new IllegalStateException(
                        "WeCom KF sync_msg returned has_more=1 without a usable next_cursor");
            }
        }
        return new SyncBatch(List.copyOf(messages), currentCursor);
    }

    void sendText(String externalUserId, String openKfid, String content)
            throws WxErrorException {
        WxCpKfTextMsg text = new WxCpKfTextMsg();
        text.setContent(content);

        WxCpKfMsgSendRequest request = new WxCpKfMsgSendRequest();
        request.setToUser(externalUserId);
        request.setOpenKfid(openKfid);
        request.setMsgType("text");
        request.setText(text);
        kfService.sendMsg(request);
    }

    private static String normalizeCursor(String cursor) {
        return cursor == null || cursor.isBlank() ? null : cursor;
    }

    record CallbackSignal(String token, String openKfid) {}

    record SyncBatch(List<WxCpKfMsgItem> messages, String nextCursor) {}
}
