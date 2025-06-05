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

package org.apache.bifromq.basekv.store.stats;

import org.apache.bifromq.base.util.AsyncRunner;
import com.google.common.collect.Maps;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class StatsCollector implements IStatsCollector {
    private final Duration interval;
    private final AsyncRunner executor;
    private final BehaviorSubject<Map<String, Double>> statsSubject = BehaviorSubject.create();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<Void> closedSignal = new CompletableFuture<>();
    private volatile long lastScrapAt = 0;

    public StatsCollector(Duration interval, Executor executor) {
        this.interval = interval;
        this.executor = new AsyncRunner("basekv.runner.statscollect", executor);
    }

    @Override
    public final Observable<Map<String, Double>> collect() {
        return statsSubject.distinctUntilChanged();
    }

    @Override
    public final void tick() {
        if (interval.compareTo(Duration.ofNanos(System.nanoTime() - lastScrapAt)) <= 0) {
            executor.add(() -> {
                if (closed.get()) {
                    if (!statsSubject.hasComplete()) {
                        statsSubject.onComplete();
                    }
                    return;
                }
                Map<String, Double> stats = Maps.newHashMap();
                scrap(stats);
                statsSubject.onNext(stats);
                lastScrapAt = System.nanoTime();
            });
        }
    }

    @Override
    public final CompletionStage<Void> stop() {
        if (closed.compareAndSet(false, true)) {
            executor.add(() -> {
                if (!statsSubject.hasComplete()) {
                    statsSubject.onComplete();
                }
                closedSignal.complete(null);
            });
        }
        return closedSignal;
    }

    protected abstract void scrap(Map<String, Double> stats);
}
