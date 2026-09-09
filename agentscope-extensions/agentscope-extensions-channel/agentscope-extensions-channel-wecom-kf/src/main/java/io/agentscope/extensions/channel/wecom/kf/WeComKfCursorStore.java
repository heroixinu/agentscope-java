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

import reactor.core.publisher.Mono;

/**
 * Shared cursor and synchronization SPI for WeCom Customer Service {@code sync_msg}.
 *
 * <p>A lease serializes one {@code channelId + openKfid} synchronization cycle. The cursor is read
 * only after the lease is acquired and is committed only after the fetched messages have been
 * processed successfully. Implementations may keep the state locally or coordinate it through a
 * shared backend such as Redis.
 */
public interface WeComKfCursorStore {

    /** Acquire exclusive synchronization ownership for one WeCom Customer Service account. */
    Mono<CursorLease> acquire(String channelId, String openKfid);

    /** Exclusive cursor lease returned by {@link #acquire(String, String)}. */
    interface CursorLease {

        /** Cursor that should be supplied to the next {@code sync_msg} call, or {@code null}. */
        String cursor();

        /** Persist the next cursor while this lease is still held. */
        Mono<Void> commit(String nextCursor);

        /** Release synchronization ownership. This method must be idempotent. */
        Mono<Void> release();
    }
}
