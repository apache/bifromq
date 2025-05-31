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
import org.apache.bifromq.basekv.store.range.KVLoadRecorder;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.function.Supplier;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class RecordingWindowSlotTest extends MockableTest {
    @Mock
    private Supplier<Long> nanoSource;

    @Test
    public void initState() {
        LoadRecordWindow slot = new LoadRecordWindow();
        assertEquals(slot.records(), 0);
        assertEquals(slot.avgLatencyNanos(), 0);
        assertEquals(slot.ioDensity(), 0);
        assertEquals(slot.ioLatencyNanos(), 0);
        assertTrue(slot.estimateSplitKey().isEmpty());
    }

    @Test
    public void record() {
        LoadRecordWindow slot = new LoadRecordWindow();
        slot.record(Collections.singletonMap(ByteString.copyFromUtf8("key1"), 10L), 1, 10, 15);
        slot.record(Collections.singletonMap(ByteString.copyFromUtf8("key1"), 20L), 1, 20, 25);
        assertEquals(slot.records(), 2);
        assertEquals(slot.ioLatencyNanos(), 15);
        assertEquals(slot.ioDensity(), 1);
        assertTrue(slot.estimateSplitKey().isPresent());
        assertEquals(slot.estimateSplitKey().get(), ByteString.copyFromUtf8("key1"));
    }

    @Test
    public void estimateSplitKey() {
        when(nanoSource.get()).thenReturn(0L);
        LoadRecordWindow windowSlot = new LoadRecordWindow();
        KVLoadRecorder recorder = new KVLoadRecorder(nanoSource);

        recorder.record(ByteString.copyFromUtf8("Key1"), 20);
        recorder.record(ByteString.copyFromUtf8("Key2"), 20);
        recorder.record(ByteString.copyFromUtf8("Key3"), 20);
        recorder.record(ByteString.copyFromUtf8("Key4"), 30);
        recorder.record(ByteString.copyFromUtf8("Key5"), 30);
        recorder.record(ByteString.copyFromUtf8("Key6"), 30);
        IKVLoadRecord record = recorder.stop();
        windowSlot.record(record.keyDistribution(), record.getKVIOs(), record.getKVIONanos(),
            nanoSource.get() - record.startNanos());
        assertTrue(windowSlot.estimateSplitKey().isPresent());
        assertEquals(windowSlot.estimateSplitKey().get(), ByteString.copyFromUtf8("Key4"));
    }

    @Test
    public void estimateSplitKeyWithNonKeyIO() {
        when(nanoSource.get()).thenReturn(0L);
        LoadRecordWindow windowSlot = new LoadRecordWindow();
        KVLoadRecorder recorder = new KVLoadRecorder(nanoSource);
        recorder.record(ByteString.copyFromUtf8("Key1"), 20);
        recorder.record(ByteString.copyFromUtf8("Key2"), 20);
        recorder.record(ByteString.copyFromUtf8("Key3"), 20);
        recorder.record(ByteString.copyFromUtf8("Key4"), 30);
        recorder.record(ByteString.copyFromUtf8("Key5"), 30);
        recorder.record(ByteString.copyFromUtf8("Key6"), 30);
        recorder.record(1000);
        IKVLoadRecord record = recorder.stop();
        windowSlot.record(record.keyDistribution(), record.getKVIOs(), record.getKVIONanos(),
            nanoSource.get() - record.startNanos());

        assertTrue(windowSlot.estimateSplitKey().isPresent());
        assertEquals(windowSlot.estimateSplitKey().get(), ByteString.copyFromUtf8("Key4"));
    }
}
