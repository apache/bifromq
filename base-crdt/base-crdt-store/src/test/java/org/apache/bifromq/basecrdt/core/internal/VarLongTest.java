/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basecrdt.core.internal;

import static org.testng.AssertJUnit.assertEquals;

import com.google.protobuf.ByteString;
import java.util.Random;
import org.testng.annotations.Test;

public class VarLongTest {

    @Test
    public void testZero() {
        long value = 0L;
        ByteString encoded = VarLong.encode(value);
        long decoded = VarLong.decode(encoded);
        assertEquals(value, decoded);
    }

    @Test
    public void testPositiveNumbers() {
        long[] testValues = {1L, 127L, 128L, 1024L, 1_000_000L, Long.MAX_VALUE / 2};
        for (long value : testValues) {
            ByteString encoded = VarLong.encode(value);
            long decoded = VarLong.decode(encoded);
            assertEquals(value, decoded);
        }
    }

    @Test
    public void testMaxValue() {
        long value = Long.MAX_VALUE;
        ByteString encoded = VarLong.encode(value);
        long decoded = VarLong.decode(encoded);
        assertEquals(value, decoded);
    }

    @Test
    public void testRandomValues() {
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            long value = random.nextLong() & Long.MAX_VALUE;
            ByteString encoded = VarLong.encode(value);
            long decoded = VarLong.decode(encoded);
            assertEquals(value, decoded);
        }
    }
}