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

package org.apache.bifromq.basekv.store.option;

import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_CHECKPOINT_ROOT_DIR;
import static org.apache.bifromq.basekv.localengine.rocksdb.RocksDBDefaultConfigs.DB_ROOT_DIR;

import com.google.protobuf.Struct;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basekv.localengine.StructUtil;
import org.apache.bifromq.basekv.localengine.spi.IKVEngineProvider;
import org.apache.bifromq.basekv.store.util.ProcessUtil;


@Accessors(chain = true)
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(toBuilder = true)
public class KVRangeStoreOptions {
    private String overrideIdentity;
    @Builder.Default
    private KVRangeOptions kvRangeOptions = new KVRangeOptions();
    @Builder.Default
    private int statsCollectIntervalSec = 5;

    // Struct-only engine spec
    @Builder.Default
    private String dataEngineType = "rocksdb";
    @Builder.Default
    private Struct dataEngineConf = defaultDataConf();

    @Builder.Default
    private String walEngineType = "rocksdb";
    @Builder.Default
    private Struct walEngineConf = defaultWalConf();

    @Builder.Default
    private Map<String, Struct> splitHinterFactoryConfig = new HashMap<>();

    private static Struct defaultDataConf() {
        // use provider defaults and set temp dirs
        IKVEngineProvider provider = findProvider("rocksdb");
        Struct.Builder b = provider.defaultsForCPable().toBuilder();
        String dbRoot = Paths.get(System.getProperty("java.io.tmpdir"), "basekv", ProcessUtil.processId(), "data")
            .toString();
        String cpRoot = Paths.get(System.getProperty("java.io.tmpdir"), "basekvcp", ProcessUtil.processId(), "data")
            .toString();
        b.putFields(DB_ROOT_DIR, StructUtil.toValue(dbRoot));
        b.putFields(DB_CHECKPOINT_ROOT_DIR, StructUtil.toValue(cpRoot));
        return b.build();
    }

    private static Struct defaultWalConf() {
        IKVEngineProvider provider = findProvider("rocksdb");
        Struct.Builder b = provider.defaultsForWALable().toBuilder();
        String dbRoot = Paths.get(System.getProperty("java.io.tmpdir"), "basekv", ProcessUtil.processId(), "wal")
            .toString();
        b.putFields(DB_ROOT_DIR, StructUtil.toValue(dbRoot));
        return b.build();
    }

    private static IKVEngineProvider findProvider(String type) {
        Map<String, IKVEngineProvider> providers = BaseHookLoader.load(IKVEngineProvider.class);
        for (IKVEngineProvider p : providers.values()) {
            if (p.type().equalsIgnoreCase(type)) {
                return p;
            }
        }
        throw new IllegalArgumentException("Unsupported storage engine type: " + type);
    }
}
