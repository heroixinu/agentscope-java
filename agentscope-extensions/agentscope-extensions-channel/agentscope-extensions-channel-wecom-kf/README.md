# AgentScope WeCom Customer Service Channel

`agentscope-extensions-channel-wecom-kf` connects AgentScope Harness Gateway to **WeCom Customer Service (企业微信 / 微信客服)** using [WxJava](https://github.com/Wechat-Group/WxJava).

This is intentionally separate from `agentscope-extensions-channel-wecom`. A normal WeCom self-built application receives the actual message in its callback. WeCom Customer Service callbacks only notify the server that new data exists; the server must call `kf/sync_msg` with the callback token and cursor to read the customer messages.

The integration goal is **AgentScope feature parity with the existing `wecom` channel**. Differences should come from the WeCom Customer Service protocol itself, not from missing AgentScope integration.

## Data flow

```text
WeChat customer
  -> WeCom KF callback (Token + OpenKfId)
  -> WeComKfCallbackController
  -> per-channel callback queue
  -> WeComKfCursorStore lease + cursor
  -> WxJava WxCpKfService.syncMsg(cursor, token, ..., open_kfid)
  -> filter origin=3 customer messages
  -> WeComKfInboundMapper
  -> ChannelRouter / bindings / DM scope
  -> Gateway.run(...)
  -> Agent reply
  -> WxJava WxCpKfService.sendMsg(...)
  -> commit next_cursor
  -> release cursor lease
```

The callback is acknowledged immediately. Sync/API calls and outbound sends are executed outside the Spring WebFlux request thread.

**No business message database is required by this adapter.** Messages fetched from `sync_msg` are passed directly into the AgentScope Gateway. Persistent runtime state is limited to the `sync_msg` cursor and, for multi-Pod deployments, the distributed synchronization lease.

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

### Feature parity with `agentscope-extensions-channel-wecom`

| AgentScope capability | `wecom` | `wecom-kf` |
| --- | --- | --- |
| `Channel.TYPE` / `ChannelFactory` creation | yes | yes |
| `init/start/stop` lifecycle | yes | yes |
| callback GET verification | yes | yes, via WxJava |
| callback POST ingestion | yes | yes, callback triggers `sync_msg` |
| process registry by `channelId` | yes | yes |
| `ChannelRouter` / bindings / default agent | yes | yes |
| DM session scope / runtime context | yes | yes |
| `Gateway.run(...)` dispatch | yes | yes |
| `IdempotencyStore` safeguard | yes | yes |
| `BotLoopGuard` | yes | yes |
| agent reply delivery | yes | yes |
| proactive `deliver()` | yes | yes, subject to KF API send window/state rules |
| Scheduler runtime registration | yes | yes |
| Scheduler callback hosting | yes | yes |
| distribution / `agentscope-all` / BOM | yes | yes |
| direct peer | yes | yes |
| group/appchat peer | yes | not applicable: WeCom Customer Service is a 1:1 customer-service protocol |

The KF-specific cursor/lease layer is an additional transport requirement; normal WeCom callbacks contain the message itself and therefore do not need it.

## Scheduler integration

The Builder `service-scheduler` treats `wecom-kf` as a first-class bundled channel alongside `wecom`:

- `SchedulerChannelRuntime` registers both channel factories;
- `SchedulerApp` explicitly hosts both callback controllers because its component-scan scope is limited to Builder packages;
- `/api/channels/wecom/**` and `/api/channels/wecom-kf/**` are public transport endpoints in Scheduler security, while each controller still verifies the WeCom cryptographic signature;
- Scheduler injects a `WeComKfCursorStore` into the KF factory. The default bean is `InMemoryWeComKfCursorStore`, and operators can replace it with `RedissonWeComKfCursorStore` without changing the channel runtime.

## Current message support

The initial adapter maps inbound and outbound **text** messages. WxJava already exposes the image, voice, video, file, location, link, mini-program and menu beans from `sync_msg`; those can be added to the mapper without changing the channel/routing architecture.

## CursorStore SPI

`WeComKfCursorStore` owns the only transport state that must survive between callbacks. Its lease deliberately combines two responsibilities that must be atomic from the channel's point of view:

1. serialize one synchronization cycle for the same `channelId + open_kfid`;
2. expose the current cursor and commit the next cursor before releasing ownership.

The normal factory remains backwards compatible and uses a process-local implementation:

```java
WeComKfChannel channel = WeComKfChannel.fromProperties(channelId, routing, properties);
```

This creates `InMemoryWeComKfCursorStore` and is appropriate for a single Pod.

### Redis / multi-Pod

For multiple Pods, create one shared `RedissonClient` in the application and inject `RedissonWeComKfCursorStore` into the channel factory or expose it as the Scheduler's `WeComKfCursorStore` bean:

```java
@Bean
WeComKfCursorStore weComKfCursorStore(RedissonClient redissonClient) {
    return new RedissonWeComKfCursorStore(redissonClient);
}
```

For direct construction:

```java
WeComKfCursorStore cursorStore = new RedissonWeComKfCursorStore(redissonClient);

WeComKfChannel channel =
        WeComKfChannel.fromProperties(channelId, routing, properties, cursorStore);
```

The default Redis key prefix is:

```text
agentscope:channel:wecom-kf
```

For each channel/account pair the implementation uses Redis Cluster hash-tagged keys equivalent to:

```text
agentscope:channel:wecom-kf:{channelId|openKfid}:cursor
agentscope:channel:wecom-kf:{channelId|openKfid}:lock
```

The Redisson lock uses an explicit owner id, so Reactor thread switches do not affect unlock ownership. Redisson's lock watchdog keeps the distributed lease alive while `sync_msg` and AgentScope processing are in progress. A callback handled by another Pod therefore waits, acquires the lease, reads the cursor committed by the previous Pod, and continues from that position.

A custom prefix can be supplied with:

```java
new RedissonWeComKfCursorStore(redissonClient, "myapp:wecom-kf");
```

`IdempotencyStore` remains as a lightweight process-local duplicate guard, but it is not used as durable message storage and is not the source of truth for synchronization progress. The Redis cursor + lease is what coordinates multi-Pod message fetching.

## WxJava

This module uses `com.github.binarywang:weixin-java-cp:4.8.6.B` and:

- `WxCpDefaultConfigImpl` for corp/secret/token/AES/API base configuration
- `WxCpServiceImpl` for access-token lifecycle and HTTP transport
- `WxCpCryptUtil` / `WxCpXmlMessage` for callback verification and decryption
- `WxCpKfService.syncMsg` for message synchronization
- `WxCpKfService.sendMsg` for replies

The Redis cursor implementation reuses the repository-managed Redisson dependency; it does not introduce Spring Data Redis or a second Redis client stack.
