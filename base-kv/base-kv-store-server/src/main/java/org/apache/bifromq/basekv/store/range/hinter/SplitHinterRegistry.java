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

package org.apache.bifromq.basekv.store.range.hinter;

import com.google.protobuf.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.slf4j.Logger;

public final class SplitHinterRegistry {
    private final List<FactorySpec> factorySpecs;

    /**
     * Load available factories and cache matched ones.
     */
    public SplitHinterRegistry(Map<String, Struct> factoryConf, Logger log) {
        Map<String, IKVRangeSplitHinterFactory> loaded = BaseHookLoader.load(IKVRangeSplitHinterFactory.class);
        List<FactorySpec> specs = new ArrayList<>();
        for (Map.Entry<String, Struct> entry : factoryConf.entrySet()) {
            String fqn = entry.getKey();
            IKVRangeSplitHinterFactory factory = loaded.get(fqn);
            if (factory == null) {
                log.warn("KVRangeSplitHinterFactory[{}] not found", fqn);
                continue;
            }
            log.info("KVRangeSplitHinterFactory[{}] enabled", fqn);
            specs.add(new FactorySpec(factory, entry.getValue()));
        }
        this.factorySpecs = specs;
    }

    /**
     * Create hinters by cached factories with incoming context.
     */
    public List<IKVRangeSplitHinter> createHinters(SplitHinterContext context) {
        List<IKVRangeSplitHinter> hinters = new ArrayList<>();
        for (FactorySpec spec : factorySpecs) {
            hinters.add(spec.factory.create(context, spec.conf));
        }
        return hinters;
    }

    private record FactorySpec(IKVRangeSplitHinterFactory factory, Struct conf) {
    }
}
