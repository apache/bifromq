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

import static org.apache.bifromq.basekv.localengine.StructUtil.strVal;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;

import com.google.protobuf.Struct;
import java.nio.file.Paths;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.slf4j.Logger;

class RocksDBWALableKVEngine extends RocksDBKVEngine<RocksDBWALableKVSpace> {

    RocksDBWALableKVEngine(String overrideIdentity, Struct conf) {
        super(overrideIdentity, conf);
    }

    @Override
    protected RocksDBWALableKVSpace doBuildKVSpace(String spaceId,
                                                   Struct conf,
                                                   Runnable onDestroy,
                                                   KVSpaceOpMeters opMeters,
                                                   Logger logger,
                                                   String... tags) {
        return new RocksDBWALableKVSpace(spaceId, conf, this, onDestroy, opMeters, logger, tags);
    }

    @Override
    protected Struct defaultConf() {
        return RocksDBDefaultConfigs.WAL;
    }

    @Override
    protected void validateSemantics(Struct conf) {
        try {
            Paths.get(strVal(conf, DB_ROOT_DIR));
        } catch (Throwable t) {
            throw new IllegalArgumentException("Invalid '" + DB_ROOT_DIR + "' path", t);
        }
    }
}
