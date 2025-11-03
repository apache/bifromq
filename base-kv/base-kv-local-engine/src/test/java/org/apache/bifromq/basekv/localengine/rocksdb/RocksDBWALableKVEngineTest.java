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

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.localengine.IKVEngine;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;

public class RocksDBWALableKVEngineTest extends AbstractRocksDBWALableEngineTest {
    protected Struct conf;

    @SneakyThrows
    @Override
    protected void beforeStart() {
        super.beforeStart();
        String DB_NAME = "testDB";
        conf = RocksDBDefaultConfigs.WAL.toBuilder()
            .putFields(RocksDBDefaultConfigs.DB_ROOT_DIR, Value.newBuilder().setStringValue(Paths.get(dbRootDir.toString(), DB_NAME).toString()).build())
            .build();
    }

    @SneakyThrows
    @Override
    protected IKVEngine<? extends IWALableKVSpace> newEngine() {
        return new RocksDBWALableKVEngine(null, conf);
    }
}
