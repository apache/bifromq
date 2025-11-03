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

package org.apache.bifromq.basekv.store;

import static org.apache.bifromq.basekv.localengine.StructUtil.strVal;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.store.option.KVRangeStoreOptions;
import org.apache.bifromq.basekv.store.stats.StatsCollector;
import org.apache.bifromq.basekv.store.util.ProcessUtil;

@Slf4j
class KVRangeStoreStatsCollector extends StatsCollector {
    private final KVRangeStoreOptions opt;

    KVRangeStoreStatsCollector(KVRangeStoreOptions opt, Duration interval, Executor executor) {
        super(interval, executor);
        this.opt = opt;
        tick();
    }

    @Override
    protected void scrap(Map<String, Double> stats) {
        if ("rocksdb".equalsIgnoreCase(opt.getDataEngineType())) {
            try {
                File dbRootDir = new File(strVal(opt.getDataEngineConf(), DB_ROOT_DIR));
                long total = dbRootDir.getTotalSpace();
                if (total > 0) {
                    stats.put("db.usage", roundUsage(dbRootDir.getUsableSpace() / (double) total));
                }
            } catch (Throwable e) {
                log.error("Failed to calculate db usage", e);
            }
        }
        if ("rocksdb".equalsIgnoreCase(opt.getWalEngineType())) {
            try {
                File walRootDir = new File(strVal(opt.getDataEngineConf(), DB_ROOT_DIR));
                long total = walRootDir.getTotalSpace();
                if (total > 0) {
                    stats.put("wal.usage", roundUsage(walRootDir.getUsableSpace() / (double) total));
                }
            } catch (Throwable e) {
                log.error("Failed to calculate wal usage", e);
            }
        }
        stats.put("cpu.usage", roundUsage(ProcessUtil.cpuLoad()));
    }

    private double roundUsage(double usage) {
        return Math.round(usage * 100.0) / 100.0;
    }
}
