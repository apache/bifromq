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

package org.apache.bifromq.starter.config.model;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;

public final class StorageEngineConfigUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private StorageEngineConfigUtil() {
    }

    @SneakyThrows
    public static StorageEngineConfig updateOrReplace(StorageEngineConfig current, Map<String, Object> conf) {
        if (conf == null) {
            return current;
        }
        Class<? extends StorageEngineConfig> desired = resolveEngineClass(conf, current);
        if (desired.isInstance(current)) {
            MAPPER.updateValue(current, conf);
            return current;
        }
        Map<String, Object> confWithType = new HashMap<>(conf);
        Object typeObj = confWithType.get("type");
        String type = typeObj == null ? null : String.valueOf(typeObj).trim();
        if (type == null || type.isEmpty()) {
            confWithType.put("type", typeNameFor(desired));
        }
        return MAPPER.convertValue(confWithType, desired);
    }

    private static Class<? extends StorageEngineConfig> resolveEngineClass(Map<String, Object> conf,
                                                                           StorageEngineConfig current) {
        Object typeObj = conf.get("type");
        String type = typeObj == null ? null : String.valueOf(typeObj).trim().toLowerCase();
        if (type == null || type.isEmpty()) {
            return current == null ? RocksDBEngineConfig.class : current.getClass();
        }
        switch (type) {
            case "memory":
                return InMemEngineConfig.class;
            case "rocksdb":
                return RocksDBEngineConfig.class;
            default:
                throw new IllegalArgumentException("Unsupported storage engine type: " + type);
        }
    }

    private static String typeNameFor(Class<? extends StorageEngineConfig> clazz) {
        // Map engine class to type id used by polymorphic deserialization
        if (clazz == InMemEngineConfig.class) {
            return "memory";
        }
        if (clazz == RocksDBEngineConfig.class) {
            return "rocksdb";
        }
        throw new IllegalArgumentException("Unsupported storage engine class: " + clazz.getName());
    }
}
