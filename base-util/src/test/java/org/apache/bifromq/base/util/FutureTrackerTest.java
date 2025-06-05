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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FutureTrackerTest {

    private FutureTracker tracker;

    @BeforeMethod
    public void setup() {
        tracker = new FutureTracker();
    }

    @Test
    public void testTrackCompletesNormally() throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();
        CompletableFuture<String> tracked = tracker.track(future);

        future.complete("done");

        assertEquals(tracked.get(), "done");

        CompletableFuture<Void> allDone = tracker.whenComplete((v, e) -> {
        });
        allDone.get();
    }

    @Test
    public void testTrackCompletesExceptionally() throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();
        CompletableFuture<String> tracked = tracker.track(future);

        Exception testEx = new RuntimeException("test exception");
        future.completeExceptionally(testEx);

        try {
            tracked.get();
            fail();
        } catch (ExecutionException ex) {
            assertEquals(testEx, ex.getCause());
        }

        CompletableFuture<Void> allDone = tracker.whenComplete((v, e) -> {
        });
        allDone.get();
    }

    @Test
    public void testStopCancelsFutures() {
        CompletableFuture<String> future = new CompletableFuture<>();
        CompletableFuture<String> tracked = tracker.track(future);

        tracker.stop();

        assertTrue(tracked.isCancelled());
    }

    @Test
    public void testWhenComplete() throws Exception {
        AtomicInteger callbackCount = new AtomicInteger(0);
        CompletableFuture<String> future1 = CompletableFuture.completedFuture("one");
        CompletableFuture<String> future2 = CompletableFuture.completedFuture("two");

        tracker.track(future1);
        tracker.track(future2);

        CompletableFuture<Void> whenDone = tracker.whenComplete((v, e) -> callbackCount.incrementAndGet());
        whenDone.get();

        assertEquals(callbackCount.get(), 1);
    }

    @Test
    public void testWhenCompleteAsync() throws Exception {
        AtomicBoolean asyncCallbackInvoked = new AtomicBoolean(false);
        CompletableFuture<String> future1 = CompletableFuture.completedFuture("one");
        CompletableFuture<String> future2 = CompletableFuture.completedFuture("two");

        tracker.track(future1);
        tracker.track(future2);

        CompletableFuture<Void> whenDoneAsync =
            tracker.whenCompleteAsync((v, e) -> asyncCallbackInvoked.set(true), Executors.newSingleThreadExecutor());
        whenDoneAsync.get();
        assertTrue(asyncCallbackInvoked.get());
    }
}