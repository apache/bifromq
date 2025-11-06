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

package org.apache.bifromq.basekv.localengine;

import com.google.protobuf.Struct;
import java.util.Map;
import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basekv.localengine.spi.IKVEngineProvider;

public class KVEngineFactory {
    public static IKVEngine<? extends ICPableKVSpace> createCPable(String overrideIdentity, String type, Struct conf) {
        Map<String, IKVEngineProvider> providers = BaseHookLoader.load(IKVEngineProvider.class);
        for (IKVEngineProvider provider : providers.values()) {
            if (provider.type().equalsIgnoreCase(type)) {
                return provider.createCPable(overrideIdentity, conf);
            }
        }
        throw new UnsupportedOperationException("No CP-able KVEngineProvider found for type: " + type);
    }

    public static IKVEngine<? extends IWALableKVSpace> createWALable(String overrideIdentity,
                                                                     String type,
                                                                     Struct conf) {
        Map<String, IKVEngineProvider> providers = BaseHookLoader.load(IKVEngineProvider.class);
        for (IKVEngineProvider provider : providers.values()) {
            if (provider.type().equalsIgnoreCase(type)) {
                return provider.createWALable(overrideIdentity, conf);
            }
        }
        throw new UnsupportedOperationException("No WAL-able KVEngineProvider found for type: " + type);
    }
}
