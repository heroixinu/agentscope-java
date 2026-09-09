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

import java.util.concurrent.ConcurrentHashMap;

/** Process-wide lookup used by the Spring callback controller. */
public final class WeComKfChannelRegistry {

    private static final WeComKfChannelRegistry INSTANCE = new WeComKfChannelRegistry();

    private final ConcurrentHashMap<String, WeComKfChannel> byChannelId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WeComKfChannel> byOpenKfid = new ConcurrentHashMap<>();

    private WeComKfChannelRegistry() {}

    public static WeComKfChannelRegistry instance() {
        return INSTANCE;
    }

    public void register(WeComKfChannel channel) {
        byChannelId.put(channel.channelId(), channel);
        byOpenKfid.put(channel.properties().openKfid(), channel);
    }

    public void unregister(String channelId) {
        WeComKfChannel removed = byChannelId.remove(channelId);
        if (removed != null) {
            byOpenKfid.remove(removed.properties().openKfid(), removed);
        }
    }

    public WeComKfChannel get(String channelId) {
        return byChannelId.get(channelId);
    }

    public WeComKfChannel getByOpenKfid(String openKfid) {
        return openKfid == null ? null : byOpenKfid.get(openKfid);
    }
}
