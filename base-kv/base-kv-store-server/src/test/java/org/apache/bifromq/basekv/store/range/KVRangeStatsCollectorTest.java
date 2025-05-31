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

package org.apache.bifromq.basekv.store.range;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.basekv.store.wal.IKVRangeWAL;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.observers.TestObserver;
import java.time.Duration;
import java.util.Map;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class KVRangeStatsCollectorTest extends MockableTest {
    @Mock
    private IKVRangeWAL rangeWAL;
    @Mock
    private IKVRangeReader rangeReader;
    @Mock
    private IKVReader kvReader;

    @Test
    public void testScrap() {
        when(rangeReader.newDataReader()).thenReturn(kvReader);
        when(kvReader.boundary()).thenReturn(FULL_BOUNDARY);
        when(kvReader.size(FULL_BOUNDARY)).thenReturn(0L);
        when(rangeWAL.logDataSize()).thenReturn(0L);
        KVRangeStatsCollector statsCollector = new KVRangeStatsCollector(rangeReader, rangeWAL,
            Duration.ofSeconds(1), MoreExecutors.directExecutor());
        TestObserver<Map<String, Double>> statsObserver = TestObserver.create();
        statsCollector.collect().subscribe(statsObserver);
        statsObserver.awaitCount(1);
        Map<String, Double> stats = statsObserver.values().get(0);
        assertEquals(0.0, stats.get("dataSize"));
        assertEquals(0.0, stats.get("walSize"));
    }
}
