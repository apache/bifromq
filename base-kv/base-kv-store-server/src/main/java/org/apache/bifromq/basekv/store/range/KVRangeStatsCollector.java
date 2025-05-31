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

import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.stats.StatsCollector;
import org.apache.bifromq.basekv.store.wal.IKVRangeWAL;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;

final class KVRangeStatsCollector extends StatsCollector {
    private final IKVRangeReader reader;

    private final IKVRangeWAL wal;

    public KVRangeStatsCollector(IKVRangeReader rangeState,
                                 IKVRangeWAL wal,
                                 Duration interval,
                                 Executor executor) {
        super(interval, executor);
        this.reader = rangeState;
        this.wal = wal;
        tick();
    }

    protected void scrap(Map<String, Double> stats) {
        stats.put("dataSize", (double) reader.size());
        stats.put("walSize", (double) wal.logDataSize());
    }
}
