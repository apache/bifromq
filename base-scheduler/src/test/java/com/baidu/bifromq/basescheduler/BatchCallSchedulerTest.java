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

package com.baidu.bifromq.basescheduler;

import com.baidu.bifromq.basescheduler.exception.ExceedLimitException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class BatchCallSchedulerTest {
    private ExecutorService executor;

    @BeforeMethod
    public void setup() {
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterMethod
    public void teardown() {
        executor.shutdown();
    }

    @SneakyThrows
    @Test
    public void batchCall() {
        TestBatchCallScheduler scheduler =
            new TestBatchCallScheduler(1, Duration.ofNanos(100), Duration.ofMillis(1), Duration.ofMillis(1));
        AtomicInteger count = new AtomicInteger(1000);
        CountDownLatch latch = new CountDownLatch(count.get());
        executor.submit(() -> {
            int i;
            while ((i = count.decrementAndGet()) >= 0) {
                int req = i;
                scheduler.schedule(i).whenComplete((v, e) -> {
                    log.info("{}: {}", req, e == null);
                    latch.countDown();
                });
            }
        });
        latch.await();
        scheduler.close();
    }

    @Test
    public void backPressure() {
        TestBatchCallScheduler scheduler = new TestBatchCallScheduler(1, Duration.ofMillis(1));
        AtomicBoolean stop = new AtomicBoolean();
        List<CompletableFuture<Integer>> respFutures = new ArrayList<>();
        int i = 0;
        while (!stop.get()) {
            int j = i++;
            CompletableFuture<Integer> respFuture = scheduler.schedule(j);
            respFutures.add(respFuture);
            respFuture.whenComplete((v, e) -> {
                if (e != null) {
                    stop.set(true);
                }
            });
        }
        try {
            log.info("Waiting for  {}", respFutures.size());
            CompletableFuture.allOf(respFutures.toArray(CompletableFuture[]::new)).join();
        } catch (Throwable e) {
            Assert.assertEquals(ExceedLimitException.class, e.getCause().getClass());
        }
    }
}
