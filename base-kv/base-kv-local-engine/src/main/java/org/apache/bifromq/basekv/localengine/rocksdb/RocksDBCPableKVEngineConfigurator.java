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

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.apache.bifromq.basekv.localengine.ICPableKVEngineConfigurator;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DBOptionsInterface;
import org.rocksdb.MutableColumnFamilyOptionsInterface;
import org.rocksdb.MutableDBOptionsInterface;

@Accessors(chain = true, fluent = true)
@Getter
@SuperBuilder(toBuilder = true)
public final class RocksDBCPableKVEngineConfigurator
    extends RocksDBKVEngineConfigurator<RocksDBCPableKVEngineConfigurator> implements ICPableKVEngineConfigurator {
    private String dbCheckpointRootDir;

    @Override
    protected void configDBOptions(DBOptionsInterface<DBOptions> targetOption) {
        super.configDBOptions(targetOption);
        // no need rocksdb wal
        targetOption.setRecycleLogFileNum(0);
        targetOption.setAllowConcurrentMemtableWrite(true);
    }

    @Override
    protected void configDBOptions(MutableDBOptionsInterface<DBOptions> targetOption) {
        super.configDBOptions(targetOption);
        targetOption.setBytesPerSync(1048576);
    }

    @Override
    protected void configCFOptions(String name, MutableColumnFamilyOptionsInterface<ColumnFamilyOptions> targetOption) {
        super.configCFOptions(name, targetOption);
        targetOption.setCompressionType(CompressionType.NO_COMPRESSION);
    }
}
