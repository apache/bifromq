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

package org.apache.bifromq.basekv;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InProcStores {
    private static final Map<String, Set<String>> IN_PROC_STORES = new ConcurrentHashMap<>();

    public static void regInProcStore(String clusterId, String storeId) {
        IN_PROC_STORES.compute(clusterId, (k, v) -> {
            if (v == null) {
                v = new HashSet<>();
            }
            v.add(storeId);
            return v;
        });
    }

    public static Set<String> getInProcStores(String clusterId) {
        return IN_PROC_STORES.getOrDefault(clusterId, Collections.emptySet());
    }
}
