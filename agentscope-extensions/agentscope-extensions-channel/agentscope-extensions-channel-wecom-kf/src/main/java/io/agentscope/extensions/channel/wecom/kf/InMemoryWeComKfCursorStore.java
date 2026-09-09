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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Process-local cursor store used by default for single-instance deployments. */
public final class InMemoryWeComKfCursorStore implements WeComKfCursorStore {

    private final ConcurrentHashMap<String, String> cursors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Semaphore> locks = new ConcurrentHashMap<>();

    @Override
    public Mono<CursorLease> acquire(String channelId, String openKfid) {
        String key = key(channelId, openKfid);
        Semaphore semaphore = locks.computeIfAbsent(key, ignored -> new Semaphore(1));
        return Mono.fromCallable(
                        () -> {
                            semaphore.acquire();
                            return (CursorLease) new Lease(key, semaphore);
                        })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private static String key(String channelId, String openKfid) {
        return channelId + "|" + openKfid;
    }

    private final class Lease implements CursorLease {
        private final String key;
        private final Semaphore semaphore;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(String key, Semaphore semaphore) {
            this.key = key;
            this.semaphore = semaphore;
        }

        @Override
        public String cursor() {
            return cursors.get(key);
        }

        @Override
        public Mono<Void> commit(String nextCursor) {
            return Mono.fromRunnable(
                    () -> {
                        if (nextCursor == null || nextCursor.isBlank()) {
                            cursors.remove(key);
                        } else {
                            cursors.put(key, nextCursor);
                        }
                    });
        }

        @Override
        public Mono<Void> release() {
            return Mono.fromRunnable(
                    () -> {
                        if (released.compareAndSet(false, true)) {
                            semaphore.release();
                        }
                    });
        }
    }
}
