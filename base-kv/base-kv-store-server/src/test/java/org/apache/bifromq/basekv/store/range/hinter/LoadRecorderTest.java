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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.store.api.IKVLoadRecord;
import org.apache.bifromq.basekv.store.range.IKVLoadRecorder;
import org.apache.bifromq.basekv.store.range.KVLoadRecorder;
import com.google.protobuf.ByteString;
import java.util.function.Supplier;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class LoadRecorderTest extends MockableTest {
    @Mock
    private Supplier<Long> nanoSource;

    @Test
    public void initState() {
        when(nanoSource.get()).thenReturn(10L);
        IKVLoadRecorder recorder = new KVLoadRecorder(nanoSource);
        IKVLoadRecord record = recorder.stop();
        assertEquals(record.getKVIONanos(), 0);
        assertEquals(record.getKVIOs(), 0);
        assertEquals(record.startNanos(), 10L);
        assertTrue(record.keyDistribution().isEmpty());
    }

    @Test
    public void record() {
        when(nanoSource.get()).thenReturn(10L);
        IKVLoadRecorder recorder = new KVLoadRecorder(nanoSource);
        recorder.record(1);
        recorder.record(ByteString.copyFromUtf8("key1"), 1L);
        recorder.record(ByteString.copyFromUtf8("key1"), 2L);
        recorder.record(ByteString.copyFromUtf8("key2"), 1L);

        IKVLoadRecord record = recorder.stop();
        assertEquals(record.getKVIONanos(), 5);
        assertEquals(record.getKVIOs(), 4);
        assertEquals(record.startNanos(), 10L);
        assertEquals(record.keyDistribution().size(), 2);
        recorder.stop();
    }
}
