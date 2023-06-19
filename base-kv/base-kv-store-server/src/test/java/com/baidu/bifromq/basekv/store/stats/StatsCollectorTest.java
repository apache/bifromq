/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basekv.store.stats;

import static org.testng.AssertJUnit.assertEquals;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.observers.TestObserver;
import java.time.Duration;
import java.util.Map;
import lombok.SneakyThrows;
import org.testng.annotations.Test;
import org.mockito.Mockito;

public class StatsCollectorTest {
    @Test
    public void initAndTick() {
        StatsCollector collector = Mockito.mock(StatsCollector.class, Mockito.withSettings()
            .useConstructor(Duration.ofSeconds(1), MoreExecutors.directExecutor())
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));

        doAnswer(invocation -> {
            Map<String, Double> map = invocation.getArgument(0);
            map.put("stat1", 0.0);
            return null;
        }).when(collector).scrap(anyMap());
        TestObserver<Map<String, Double>> statsObserver = TestObserver.create();
        collector.collect().subscribe(statsObserver);
        collector.tick();
        statsObserver.awaitCount(1);
        assertEquals(0.0d, statsObserver.values().get(0).get("stat1").doubleValue(), 0.0);
    }

    @SneakyThrows
    @Test
    public void tickInterval() {
        StatsCollector collector = Mockito.mock(StatsCollector.class, Mockito.withSettings()
            .useConstructor(Duration.ofMillis(500), MoreExecutors.directExecutor())
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
        AtomicDouble statValue = new AtomicDouble();
        doAnswer(invocation -> {
            Map<String, Double> map = invocation.getArgument(0);
            map.put("stat1", statValue.getAndAdd(1.0));
            return null;
        }).when(collector).scrap(anyMap());
        TestObserver<Map<String, Double>> statsObserver = TestObserver.create();
        collector.collect().subscribe(statsObserver);
        collector.tick();
        collector.tick();
        Thread.sleep(550);
        collector.tick();
        collector.stop().toCompletableFuture().join();
        statsObserver.await();
        assertEquals(2, statsObserver.values().size());
    }

    @SneakyThrows
    @Test
    public void distinctUntilChange() {
        StatsCollector collector = Mockito.mock(StatsCollector.class, Mockito.withSettings()
            .useConstructor(Duration.ofMillis(10), MoreExecutors.directExecutor())
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
        doAnswer(invocation -> {
            Map<String, Double> map = invocation.getArgument(0);
            map.put("stat1", 0.0);
            return null;
        }).when(collector).scrap(anyMap());
        TestObserver<Map<String, Double>> statsObserver = TestObserver.create();
        collector.collect().subscribe(statsObserver);
        collector.tick();
        Thread.sleep(10);
        collector.tick();
        Thread.sleep(10);
        collector.tick();
        collector.stop().toCompletableFuture().join();
        statsObserver.await();
        assertEquals(1, statsObserver.values().size());
    }
}
