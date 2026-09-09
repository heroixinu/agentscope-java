# AgentScope WeCom Customer Service Channel

`agentscope-extensions-channel-wecom-kf` connects AgentScope Harness Gateway to **WeCom Customer Service (企业微信 / 微信客服)** using [WxJava](https://github.com/binarywang/WxJava).

This is intentionally separate from `agentscope-extensions-channel-wecom`. A normal WeCom self-built application receives the actual message in its callback. WeCom Customer Service callbacks only notify the server that new data exists; the server must call `kf/sync_msg` with the callback token and cursor to read the customer messages.

## Data flow

```text
WeChat customer
  -> WeCom KF callback (Token + OpenKfId)
  -> WeComKfCallbackController
  -> per-channel serialized callback queue
  -> WxJava WxCpKfService.syncMsg(cursor, token, ..., open_kfid)
  -> filter origin=3 customer messages
  -> WeComKfInboundMapper
  -> ChannelRouter / bindings / DM scope
  -> Gateway.run(...)
  -> Agent reply
  -> WxJava WxCpKfService.sendMsg(...)
  -> WeChat customer
```

The callback is acknowledged immediately. Sync/API calls and outbound sends are blocking WxJava operations and are moved to Reactor `boundedElastic`, so the Spring WebFlux request thread is not blocked.

## Configuration

Create one AgentScope channel per WeCom Customer Service account (`open_kfid`). Example properties:

```json
{
  "corpId": "wwxxxxxxxxxxxxxxxx",
  "secret": "${WECOM_KF_SECRET}",
  "token": "${WECOM_KF_CALLBACK_TOKEN}",
  "encodingAesKey": "${WECOM_KF_ENCODING_AES_KEY}",
  "openKfid": "wkxxxxxxxxxxxxxxxx",
  "syncLimit": 1000,
  "voiceFormat": 0
}
```

Channel type: `wecom-kf`.

Default callback path:

```text
/api/channels/wecom-kf/{channelId}/callback
```

Configure that URL in the WeCom Customer Service callback settings. For multiple `open_kfid` accounts in the same corporation, the callback controller inspects the decrypted `OpenKfId` and routes the notification to the registered channel for that account. This lets one enterprise-level callback URL serve multiple AgentScope channel instances as long as they share the same callback crypto configuration.

## AgentScope integration

Inbound messages are normalized as:

- `channelId`: configured AgentScope channel id
- `accountId`: `open_kfid`
- `peer`: `DIRECT:<external_userid>`
- `senderId`: `external_userid`

That means the standard AgentScope `ChannelRouter` continues to provide bindings, default-agent resolution, DM session scope, user isolation, runtime context and outbound addressing. `deliver()` also works for proactive direct delivery when the WeCom Customer Service API permits it.

Only `origin=3` records are dispatched. `origin=4` system/events and `origin=5` servicer messages are ignored so an agent reply cannot be re-consumed as user input.

## Current message support

The initial adapter maps inbound and outbound **text** messages. WxJava already exposes the image, voice, video, file, location, link, mini-program and menu beans from `sync_msg`; those can be added to the mapper without changing the channel/routing architecture.

## Runtime state

The `sync_msg` cursor is kept per channel instance in memory and callback processing is serialized per channel. `MsgId` is additionally deduplicated with the common AgentScope `IdempotencyStore`.

For multi-Pod deployments, provide external callback affinity or evolve the cursor/idempotency layer to a shared store before allowing multiple Pods to consume callbacks for the same `open_kfid`.

## WxJava

This module pins `com.github.binarywang:weixin-java-cp:4.8.5` and uses:

- `WxCpDefaultConfigImpl` for corp/secret/token/AES/API base configuration
- `WxCpServiceImpl` for access-token lifecycle and HTTP transport
- `WxCpCryptUtil` / `WxCpXmlMessage` for callback verification and decryption
- `WxCpKfService.syncMsg` for message synchronization
- `WxCpKfService.sendMsg` for replies
