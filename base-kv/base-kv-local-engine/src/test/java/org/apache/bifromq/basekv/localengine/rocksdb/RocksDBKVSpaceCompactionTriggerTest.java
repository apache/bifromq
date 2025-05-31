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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.bifromq.basekv.localengine.MockableTest;
import java.util.stream.IntStream;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class RocksDBKVSpaceCompactionTriggerTest extends MockableTest {
    @Mock
    private Runnable compact;

    @Test
    public void noCompactTriggered() {
        RocksDBKVSpaceCompactionTrigger trigger = new RocksDBKVSpaceCompactionTrigger("id", 10, 10, 0.3, compact);
        trigger.newRecorder().stop();
        verify(compact, times(0)).run();
    }

    @Test
    public void reset() {
        RocksDBKVSpaceCompactionTrigger trigger = new RocksDBKVSpaceCompactionTrigger("id", 10, 10, 0.3, compact);
        IWriteStatsRecorder.IRecorder recorder = trigger.newRecorder();
        IntStream.range(0, 11).forEach(i -> recorder.recordDelete());
        recorder.stop();
        verify(compact, times(1)).run();
        // trigger again before reset
        trigger.newRecorder().stop();
        verify(compact, times(2)).run();

        trigger.reset();
        trigger.newRecorder().stop();
        verify(compact, times(2)).run();
    }

    @Test
    public void triggeredByMinTombstoneKeys() {
        RocksDBKVSpaceCompactionTrigger trigger = new RocksDBKVSpaceCompactionTrigger("id", 10, 10, 0.3, compact);
        IWriteStatsRecorder.IRecorder recorder = trigger.newRecorder();
        IntStream.range(0, 11).forEach(i -> recorder.recordDelete());
        recorder.stop();
        verify(compact, times(1)).run();
    }

    @Test
    public void triggeredByMinTombstoneRanges() {
        RocksDBKVSpaceCompactionTrigger trigger = new RocksDBKVSpaceCompactionTrigger("id", 10, 10, 0.3, compact);
        IWriteStatsRecorder.IRecorder recorder = trigger.newRecorder();
        IntStream.range(0, 11).forEach(i -> recorder.recordDeleteRange());
        recorder.stop();
        verify(compact, times(1)).run();
    }

    @Test
    public void triggerByMinTombstoneRatio_put() {
        RocksDBKVSpaceCompactionTrigger trigger = new RocksDBKVSpaceCompactionTrigger("id", 10, 10, 0.3, compact);
        IWriteStatsRecorder.IRecorder recorder = trigger.newRecorder();
        IntStream.range(0, 11).forEach(i -> recorder.recordPut());
        recorder.stop();
        verify(compact, times(1)).run();
    }

    @Test
    public void triggerByMinTombstoneRatio_insertAndDelete() {
        RocksDBKVSpaceCompactionTrigger trigger = new RocksDBKVSpaceCompactionTrigger("id", 10, 10, 0.3, compact);
        IWriteStatsRecorder.IRecorder recorder = trigger.newRecorder();
        IntStream.range(0, 11).forEach(i -> recorder.recordDelete());
        IntStream.range(0, 20).forEach(i -> recorder.recordPut());
        recorder.stop();
        verify(compact, times(1)).run();
    }
}
