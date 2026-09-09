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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import reactor.core.publisher.Mono;

/**
 * Redis-backed WeCom KF cursor store implemented with Redisson.
 *
 * <p>The Redis lock is acquired with an explicit owner id so acquisition and release are safe even
 * when Reactor resumes on a different Java thread. Redisson's lock watchdog keeps the lease alive
 * while the synchronization cycle is running. The shared cursor is committed before the lock is
 * released.
 */
public final class RedissonWeComKfCursorStore implements WeComKfCursorStore {

    public static final String DEFAULT_KEY_PREFIX = "agentscope:channel:wecom-kf";

    private static final AtomicLong OWNER_IDS = new AtomicLong();

    private final RedissonClient redisson;
    private final String keyPrefix;

    public RedissonWeComKfCursorStore(RedissonClient redisson) {
        this(redisson, DEFAULT_KEY_PREFIX);
    }

    public RedissonWeComKfCursorStore(RedissonClient redisson, String keyPrefix) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        if (keyPrefix == null || keyPrefix.isBlank()) {
            throw new IllegalArgumentException("keyPrefix must not be blank");
        }
        this.keyPrefix = trimTrailingColon(keyPrefix);
    }

    @Override
    public Mono<CursorLease> acquire(String channelId, String openKfid) {
        String stateKey = stateKey(channelId, openKfid);
        RLock lock = redisson.getLock(stateKey + ":lock");
        RBucket<String> cursorBucket = redisson.getBucket(stateKey + ":cursor");
        long ownerId = OWNER_IDS.incrementAndGet();

        return Mono.defer(
                () ->
                        Mono.fromCompletionStage(lock.lockAsync(ownerId))
                                .then(
                                        Mono.fromCompletionStage(
                                                cursorBucket
                                                        .getAsync()
                                                        .thenApply(Optional::ofNullable)))
                                .map(
                                        cursor ->
                                                (CursorLease)
                                                        new Lease(
                                                                lock,
                                                                cursorBucket,
                                                                ownerId,
                                                                cursor.orElse(null)))
                                .onErrorResume(
                                        error ->
                                                Mono.fromCompletionStage(
                                                                lock.unlockAsync(ownerId))
                                                        .onErrorResume(unlockError -> Mono.empty())
                                                        .then(Mono.error(error))));
    }

    private String stateKey(String channelId, String openKfid) {
        if (channelId == null || channelId.isBlank()) {
            throw new IllegalArgumentException("channelId must not be blank");
        }
        if (openKfid == null || openKfid.isBlank()) {
            throw new IllegalArgumentException("openKfid must not be blank");
        }
        // The hash tag keeps the cursor and lock in the same Redis Cluster slot.
        return keyPrefix + ":{" + channelId + "|" + openKfid + "}";
    }

    private static String trimTrailingColon(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == ':') {
            end--;
        }
        return value.substring(0, end);
    }

    private static final class Lease implements CursorLease {
        private final RLock lock;
        private final RBucket<String> cursorBucket;
        private final long ownerId;
        private final String cursor;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(
                RLock lock, RBucket<String> cursorBucket, long ownerId, String cursor) {
            this.lock = lock;
            this.cursorBucket = cursorBucket;
            this.ownerId = ownerId;
            this.cursor = cursor;
        }

        @Override
        public String cursor() {
            return cursor;
        }

        @Override
        public Mono<Void> commit(String nextCursor) {
            if (released.get()) {
                return Mono.error(new IllegalStateException("cursor lease already released"));
            }
            if (nextCursor == null || nextCursor.isBlank()) {
                return Mono.fromCompletionStage(cursorBucket.deleteAsync()).then();
            }
            return Mono.fromCompletionStage(cursorBucket.setAsync(nextCursor));
        }

        @Override
        public Mono<Void> release() {
            if (!released.compareAndSet(false, true)) {
                return Mono.empty();
            }
            return Mono.fromCompletionStage(lock.unlockAsync(ownerId));
        }
    }
}
