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

package org.apache.bifromq.basekv.localengine.rocksdb;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.apache.bifromq.basekv.localengine.IWALableKVEngineConfigurator;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;

@Accessors(chain = true, fluent = true)
@Getter
@SuperBuilder(toBuilder = true)
public final class RocksDBWALableKVEngineConfigurator
    extends RocksDBKVEngineConfigurator<RocksDBWALableKVEngineConfigurator> implements IWALableKVEngineConfigurator {
    @Builder.Default
    private boolean asyncWALFlush = false;
    @Builder.Default
    private boolean fsyncWAL = false;

    @Override
    public DBOptions dbOptions() {
        DBOptions options = super.dbOptions();
        options.setManualWalFlush(asyncWALFlush);
        options.setBytesPerSync(1048576);
        options.setAllowConcurrentMemtableWrite(true);
        return options;
    }

    @Override
    public ColumnFamilyOptions cfOptions(String name) {
        ColumnFamilyOptions options = super.cfOptions(name);
        options.setCompressionType(CompressionType.NO_COMPRESSION);
        options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        options.setLevelCompactionDynamicLevelBytes(true);
        options.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
        return options;
    }
}
