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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.testng.annotations.Test;

public class CompletableFutureUtilTest {

    @Test
    void testUnwrap_BiConsumer() {
        AtomicReference<Throwable> captured = new AtomicReference<>();

        BiConsumer<String, Throwable> original = (res, ex) -> captured.set(ex);

        BiConsumer<String, Throwable> wrapped = CompletableFutureUtil.unwrap(original);

        IllegalStateException cause = new IllegalStateException("boom");
        wrapped.accept("ignored", new CompletionException(cause));
        assertSame(cause, captured.get());

        IllegalArgumentException iae = new IllegalArgumentException("arg");
        wrapped.accept("ignored", iae);
        assertSame(iae, captured.get());

        captured.set(new RuntimeException());
        wrapped.accept("ignored", null);
        assertNull(captured.get());
    }

    @Test
    void testUnwrap_BiFunction() {
        BiFunction<String, Throwable, String> originalFn =
            (res, ex) -> ex == null ? res : ex.getClass().getSimpleName();

        BiFunction<String, Throwable, String> wrappedFn = CompletableFutureUtil.unwrap(originalFn);

        IllegalStateException cause = new IllegalStateException();
        String result1 = wrappedFn.apply("OK", new CompletionException(cause));
        assertEquals(result1, "IllegalStateException");

        String result2 = wrappedFn.apply("HELLO", null);
        assertEquals(result2, "HELLO");
    }

    @Test
    void testUnwrap_Function() {
        AtomicReference<Throwable> captured = new AtomicReference<>();

        Function<Throwable, Object> original = (e) -> {
            captured.set(e);
            return null;
        };
        Function<Throwable, Object> wrapped = CompletableFutureUtil.unwrap(original);

        IllegalStateException cause = new IllegalStateException("fail");
        wrapped.apply(new CompletionException(cause));
        assertSame(cause, captured.get());

        IllegalArgumentException iae = new IllegalArgumentException("arg");
        wrapped.apply(iae);
        assertSame(iae, captured.get());

        captured.set(new RuntimeException());
        wrapped.apply(null);
        assertNull(captured.get());
    }
}