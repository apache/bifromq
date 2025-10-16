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

package org.apache.bifromq.basekv.store.range;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.bifromq.basekv.store.stats.StatsCollector;
import org.apache.bifromq.basekv.store.wal.IKVRangeWAL;

class KVRangeStatsCollector extends StatsCollector {
    private final IKVRange reader;

    private final IKVRangeWAL wal;

    public KVRangeStatsCollector(IKVRange rangeState, IKVRangeWAL wal, Duration interval, Executor executor) {
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
