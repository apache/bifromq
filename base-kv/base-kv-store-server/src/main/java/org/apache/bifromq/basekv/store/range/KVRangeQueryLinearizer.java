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

package org.apache.bifromq.basekv.store.range;

import com.google.common.collect.Maps;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class KVRangeQueryLinearizer implements IKVRangeQueryLinearizer {
    private final Logger log;
    private final ConcurrentMap<CompletableFuture<Long>, CompletableFuture<Void>> readIndexes = Maps.newConcurrentMap();
    private final ConcurrentLinkedDeque<ToLinearize> toBeLinearized = new ConcurrentLinkedDeque<>();
    private final Supplier<CompletableFuture<Long>> readIndexProvider;
    private final Executor executor;
    private final AtomicBoolean linearizing = new AtomicBoolean();
    private volatile long lastAppliedIndex = 0;

    KVRangeQueryLinearizer(Supplier<CompletableFuture<Long>> readIndexProvider, Executor executor,
                           long lastAppliedIndex, String... tags) {
        this.readIndexProvider = readIndexProvider;
        this.executor = executor;
        this.lastAppliedIndex = lastAppliedIndex;
        this.log = MDCLogger.getLogger(KVRangeQueryLinearizer.class, tags);
    }

    @Override
    public CompletionStage<Void> linearize() {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        CompletableFuture<Long> readIndex = readIndexProvider.get();
        readIndexes.put(readIndex, onDone);
        readIndex.whenCompleteAsync((ri, e) -> {
            if (e != null) {
                log.debug("failed to get readIndex", e);
                readIndexes.remove(readIndex).completeExceptionally(e);
            } else {
                if (ri <= lastAppliedIndex) {
                    readIndexes.remove(readIndex).complete(null);
                } else {
                    readIndexes.remove(readIndex, onDone);
                    if (!onDone.isDone()) {
                        toBeLinearized.add(new ToLinearize(ri, onDone));
                        schedule();
                    }
                }
            }
        }, executor);
        return onDone;
    }

    @Override
    public void afterLogApplied(long logIndex) {
        if (logIndex >= lastAppliedIndex) {
            lastAppliedIndex = logIndex;
            schedule();
        }
    }

    private void schedule() {
        if (linearizing.compareAndSet(false, true)) {
            executor.execute(this::doLinearize);
        }
    }

    private void doLinearize() {
        ToLinearize toLinearize;
        while ((toLinearize = toBeLinearized.poll()) != null) {
            if (toLinearize.readIndex <= lastAppliedIndex) {
                toLinearize.onDone.complete(null);
            } else {
                // put it back
                toBeLinearized.addFirst(toLinearize);
                break;
            }
        }
        linearizing.set(false);
        if ((toLinearize = toBeLinearized.peek()) != null && toLinearize.readIndex <= lastAppliedIndex) {
            schedule();
        }
    }

    @AllArgsConstructor
    private static class ToLinearize {
        final long readIndex;
        final CompletableFuture<Void> onDone;
    }
}
