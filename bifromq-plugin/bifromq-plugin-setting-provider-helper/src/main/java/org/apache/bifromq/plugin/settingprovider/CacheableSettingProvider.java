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

package org.apache.bifromq.plugin.settingprovider;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.util.EnumMap;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CacheableSettingProvider implements ISettingProvider {
    private final EnumMap<Setting, LoadingCache<String, Object>> settingCache = new EnumMap<>(Setting.class);
    private final ISettingProvider delegate;

    public CacheableSettingProvider(ISettingProvider delegate, CacheOptions options) {
        this.delegate = delegate;
        for (Setting setting : Setting.values()) {
            settingCache.put(setting, Caffeine.newBuilder()
                .expireAfterAccess(options.expireDuration())
                .maximumSize(options.maxCachedTenants())
                .refreshAfterWrite(options.refreshDuration())
                .scheduler(Scheduler.systemScheduler())
                .build(buildCacheLoader(setting, options.provideInitValue())));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R provide(Setting setting, String tenantId) {
        return (R) settingCache.get(setting).get(tenantId);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private CacheLoader<String, Object> buildCacheLoader(Setting setting, boolean provideInitValue) {
        if (provideInitValue) {
            return new CacheLoader<>() {
                @Override
                public @Nullable Object load(String tenantId) {
                    Object val = delegate.provide(setting, tenantId);
                    return val == null ? setting.initialValue() : val;
                }

                @Override
                public @Nullable Object reload(String tenantId, Object oldValue) {
                    // using provider to get the latest value
                    Object val = delegate.provide(setting, tenantId);
                    return val == null ? oldValue : val;
                }
            };
        }
        return new CacheLoader<>() {
            @Override
            public @Nullable Object load(String tenantId) {
                return setting.initialValue();
            }

            @Override
            public @Nullable Object reload(String tenantId, Object oldValue) {
                // using provider to get the latest value
                Object val = delegate.provide(setting, tenantId);
                return val == null ? oldValue : val;
            }
        };
    }
}
