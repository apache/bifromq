/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bifromq.base.util;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 * FutureTracker is a utility class that allows for tracking and managing multiple CompletableFutures.
 */
public final class FutureTracker {
    private final Set<CompletableFuture<?>> track = ConcurrentHashMap.newKeySet();

    /**
     * Tracks a CompletableFuture and removes it from the tracking set when it completes.
     *
     * @param trackedFuture the CompletableFuture to track
     * @return the tracked CompletableFuture
     */
    public <T> CompletableFuture<T> track(CompletableFuture<T> trackedFuture) {
        track.add(trackedFuture);
        trackedFuture.whenComplete((v, e) -> track.remove(trackedFuture));
        return trackedFuture;
    }

    /**
     * Stops tracking all futures and cancels them.
     */
    public void stop() {
        for (CompletableFuture<?> tracked : track) {
            tracked.cancel(true);
        }
    }

    public CompletableFuture<Void> whenComplete(BiConsumer<Void, Throwable> biConsumer) {
        return CompletableFuture.allOf(track.toArray(new CompletableFuture[0]))
            .whenComplete(biConsumer);
    }

    public CompletableFuture<Void> whenCompleteAsync(BiConsumer<Void, Throwable> biConsumer, Executor executor) {
        return CompletableFuture.allOf(track.toArray(new CompletableFuture[0]))
            .whenCompleteAsync(biConsumer, executor);
    }
}
