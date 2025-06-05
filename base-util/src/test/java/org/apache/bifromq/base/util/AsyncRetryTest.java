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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertTrue;

import org.apache.bifromq.base.util.exception.RetryTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.testng.annotations.Test;

public class AsyncRetryTest {

    @Test
    public void testImmediateSuccess() throws Exception {
        String expected = "success";
        Supplier<CompletableFuture<String>> taskSupplier = () -> CompletableFuture.completedFuture(expected);
        CompletableFuture<String> resultFuture =
            AsyncRetry.exec(taskSupplier, (result, e) -> !result.equals("success"), 100, 1000);
        String result = resultFuture.get();
        assertEquals(result, expected);
    }

    @Test
    public void testNoRetry() {
        Supplier<CompletableFuture<String>> taskSupplier = () -> CompletableFuture.failedFuture(new RuntimeException());
        CompletableFuture<String> resultFuture =
            AsyncRetry.exec(taskSupplier, (result, e) -> e == null, 0, 1000);
        assertThrows(resultFuture::join);
    }

    @Test
    public void testRetriesThenSuccess() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);
        Supplier<CompletableFuture<String>> taskSupplier = () -> CompletableFuture.supplyAsync(() -> {
            int attempt = counter.incrementAndGet();
            return attempt < 3 ? "fail" : "success";
        });

        CompletableFuture<String> resultFuture =
            AsyncRetry.exec(taskSupplier, (result, e) -> !result.equals("success"), 100, 1000);
        String result = resultFuture.get();
        assertEquals(result, "success");
        assertTrue(counter.get() >= 3);
    }

    @Test
    public void testTimeoutExceeded() throws Exception {
        AtomicInteger counter = new AtomicInteger(0);
        Supplier<CompletableFuture<String>> taskSupplier = () -> CompletableFuture.supplyAsync(() -> {
            counter.incrementAndGet();
            return "fail";
        });

        CompletableFuture<String> resultFuture =
            AsyncRetry.exec(taskSupplier, (result, e) -> !result.equals("success"), 100, 250);

        try {
            resultFuture.get();
            fail();
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            assertTrue(cause instanceof RetryTimeoutException);
        }
        assertTrue(counter.get() > 0);
    }
}