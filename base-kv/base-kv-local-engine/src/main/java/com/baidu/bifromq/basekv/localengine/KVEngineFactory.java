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

package com.baidu.bifromq.basekv.localengine;

import com.baidu.bifromq.basekv.localengine.memory.InMemCPableKVEngine;
import com.baidu.bifromq.basekv.localengine.memory.InMemKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.memory.InMemWALableKVEngine;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngine;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngine;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBCPableKVEngineConfigurator;
import com.baidu.bifromq.basekv.localengine.rocksdb.RocksDBWALableKVEngineConfigurator;

public class KVEngineFactory {
    public static IKVEngine<? extends ICPableKVSpace> createCPable(String overrideIdentity,
                                                                   ICPableKVEngineConfigurator configurator) {
        if (configurator instanceof InMemKVEngineConfigurator) {
            return new InMemCPableKVEngine(overrideIdentity, (InMemKVEngineConfigurator) configurator);
        }
        if (configurator instanceof RocksDBCPableKVEngineConfigurator) {
            return new RocksDBCPableKVEngine(overrideIdentity, (RocksDBCPableKVEngineConfigurator) configurator);
        }
        throw new UnsupportedOperationException();
    }

    public static IKVEngine<? extends IWALableKVSpace> createWALable(String overrideIdentity,
                                                                     IWALableKVEngineConfigurator configurator) {
        if (configurator instanceof InMemKVEngineConfigurator) {
            return new InMemWALableKVEngine(overrideIdentity, (InMemKVEngineConfigurator) configurator);
        }
        if (configurator instanceof RocksDBWALableKVEngineConfigurator) {
            return new RocksDBWALableKVEngine(overrideIdentity, (RocksDBWALableKVEngineConfigurator) configurator);
        }
        throw new UnsupportedOperationException();
    }
}
