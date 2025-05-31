/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basescheduler;

import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TestBatchCallScheduler extends BatchCallScheduler<Integer, Integer, Integer> {
    private final int queueNum;

    public TestBatchCallScheduler(int queues, Duration callDelay) {
        this(queues, callDelay, callDelay.multipliedBy(2));
    }

    public TestBatchCallScheduler(int queues, Duration callDelay, Duration burstLatency) {
        super((n, batcherKey) -> () -> new TestBatchCall(callDelay), burstLatency.toNanos());
        this.queueNum = queues;
    }

    @Override
    protected Optional<Integer> find(Integer request) {
        return Optional.of(ThreadLocalRandom.current().nextInt(queueNum));
    }

    public static class TestBatchCall implements IBatchCall<Integer, Integer, Integer> {
        private final Queue<ICallTask<Integer, Integer, Integer>> batch = new ConcurrentLinkedQueue<>();
        private final Duration callDelay;

        public TestBatchCall(Duration callDelay) {
            this.callDelay = callDelay;
        }

        @Override
        public void reset() {
            batch.clear();
        }

        @Override
        public void add(ICallTask<Integer, Integer, Integer> callTask) {
            log.info("{}: {}", callTask.batcherKey(), callTask.call());
            batch.add(callTask);
        }

        @Override
        public CompletableFuture<Void> execute() {
            return CompletableFuture.runAsync(() -> {
                for (ICallTask<Integer, Integer, Integer> task : batch) {
                    task.resultPromise().complete(task.call());
                }
            }, CompletableFuture.delayedExecutor(callDelay.toMillis(), TimeUnit.MILLISECONDS));
        }
    }
}
