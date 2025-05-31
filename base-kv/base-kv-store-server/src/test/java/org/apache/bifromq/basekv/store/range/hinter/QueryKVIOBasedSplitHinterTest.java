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

package org.apache.bifromq.basekv.store.range.hinter;

import static org.apache.bifromq.basekv.store.range.hinter.KVLoadBasedSplitHinter.LOAD_TYPE_IO_DENSITY;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.proto.SplitHint;
import org.apache.bifromq.basekv.store.range.IKVLoadRecorder;
import org.apache.bifromq.basekv.store.range.KVLoadRecorder;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class QueryKVIOBasedSplitHinterTest extends MockableTest {
    @Mock
    protected Supplier<Long> nanoSource;

    @Test
    public void noLoadRecorded() {
        QueryKVLoadBasedSplitHinter testEstimator =
            new QueryKVLoadBasedSplitHinter(Duration.ofSeconds(5), Optional::of);
        SplitHint hint = testEstimator.estimate();
        assertFalse(hint.hasSplitKey());
    }

    @Test
    public void hintMemoization() {
        QueryKVLoadBasedSplitHinter estimator =
            new QueryKVLoadBasedSplitHinter(nanoSource, Duration.ofSeconds(5), Optional::of);
        long now = 0L;
        // track enough records
        for (int i = 0; i < 11; i++) {
            when(nanoSource.get()).thenReturn(now);
            IKVLoadRecorder recorder = new KVLoadRecorder(nanoSource);
            recorder.record(ByteString.copyFromUtf8("Key"), 10_000);
            when(nanoSource.get()).thenReturn(now + 10_000);
            estimator.recordQuery(null, recorder.stop());
        }
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(6).toNanos());
        SplitHint hint1 = estimator.estimate();
        SplitHint hint2 = estimator.estimate();
        assertTrue(hint1.hasSplitKey());
        assertSame(hint1, hint2);
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(11).toNanos());
        hint2 = estimator.estimate();
        assertNotSame(hint1, hint2);
    }

    @Test
    public void trackClearExpiredSlots() {
        QueryKVLoadBasedSplitHinter estimator =
            new QueryKVLoadBasedSplitHinter(nanoSource, Duration.ofSeconds(1), Optional::of);
        long now = 0L;
        for (int i = 0; i < 11; i++) {
            when(nanoSource.get()).thenReturn(now);
            IKVLoadRecorder recorder = new KVLoadRecorder(nanoSource);
            recorder.record(ByteString.copyFromUtf8("Key"), Duration.ofMillis(10).toNanos());
            when(nanoSource.get()).thenReturn(now + Duration.ofMillis(200).toNanos());
            estimator.recordQuery(null, recorder.stop());
        }
        when(nanoSource.get()).thenReturn(now + Duration.ofMillis(1000).toNanos());
        SplitHint hint = estimator.estimate();
        assertTrue(hint.getLoadMap().get(LOAD_TYPE_IO_DENSITY) > 0);
        assertTrue(hint.hasSplitKey());
        when(nanoSource.get()).thenReturn(now + Duration.ofSeconds(2).toNanos());
        hint = estimator.estimate();
        assertEquals(hint.getLoadMap().getOrDefault(LOAD_TYPE_IO_DENSITY, 0d), 0d);
        assertFalse(hint.hasSplitKey());
    }
}
