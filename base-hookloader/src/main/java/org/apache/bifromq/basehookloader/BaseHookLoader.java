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

package org.apache.bifromq.basehookloader;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class BaseHookLoader {
    private static final Map<Class<?>, Map<String, ?>> LOADED = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public static <T> Map<String, T> load(Class<T> hookInterface) {
        return (Map<String, T>) LOADED.computeIfAbsent(hookInterface, k -> {
            Map<String, T> loadedFactories = new HashMap<>();
            ServiceLoader<T> serviceLoader = ServiceLoader.load(hookInterface);
            Iterator<T> iterator = serviceLoader.iterator();
            iterator.forEachRemaining(factoryImpl -> {
                String className = factoryImpl.getClass().getName();
                if (className.trim().isEmpty()) {
                    throw new IllegalStateException("Anonymous implementation is not allowed");
                }
                log.debug("Loaded {} implementation: {}", hookInterface.getSimpleName(), className);
                if (loadedFactories.putIfAbsent(className, factoryImpl) != null) {
                    throw new IllegalStateException("More than one implementations using same name " + className);
                }
            });
            return loadedFactories;
        });
    }
}