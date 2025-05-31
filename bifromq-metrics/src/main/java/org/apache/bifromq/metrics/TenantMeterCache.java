/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.metrics;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class TenantMeterCache {

    private static class CleanableState implements Runnable {

        final String tenantId;
        final Map<String, WeakReference<TenantMeter>> tenantMeterWeakRefMap;

        private CleanableState(String tenantId, Map<String, WeakReference<TenantMeter>> tenantMeterWeakRefMap) {
            this.tenantId = tenantId;
            this.tenantMeterWeakRefMap = tenantMeterWeakRefMap;
        }

        @Override
        public void run() {
            tenantMeterWeakRefMap.remove(tenantId);
        }
    }

    private static final Cleaner CLEANER = Cleaner.create();

    private static class ThreadLocalRef {

        private final Function<String, TenantMeter> tenantMeterGetter;
        private final Map<String, WeakReference<TenantMeter>> tenantMeterWeakRefMap;
        private final WeakHashMap<TenantMeter, CleanableState> cleanableStateMap;

        private ThreadLocalRef(Function<String, TenantMeter> tenantMeterGetter) {
            this.tenantMeterGetter = tenantMeterGetter;
            tenantMeterWeakRefMap = new ConcurrentHashMap<>();
            cleanableStateMap = new WeakHashMap<>();
        }

        public TenantMeter get(String tenantId) {
            TenantMeter tenantMeter;
            while ((tenantMeter = getInternal(tenantId)) == null) {
                // get again
            }
            return tenantMeter;
        }

        private TenantMeter getInternal(String tenantId) {
            return tenantMeterWeakRefMap.computeIfAbsent(tenantId, k -> {
                TenantMeter meter = tenantMeterGetter.apply(k);
                CleanableState state = new CleanableState(k, tenantMeterWeakRefMap);
                cleanableStateMap.put(meter, state);
                CLEANER.register(meter, state);
                return new WeakReference<>(meter);
            }).get();
        }
    }

    private static final LoadingCache<String, TenantMeter> TENANT_METER_CACHE = Caffeine.newBuilder()
        .weakValues()
        .build(TenantMeter::new);
    private static final ThreadLocal<ThreadLocalRef> THREAD_LOCAL_REF;

    static {
        THREAD_LOCAL_REF = ThreadLocal.withInitial(() -> new ThreadLocalRef(TENANT_METER_CACHE::get));
    }

    static ITenantMeter get(String tenantId) {
        return THREAD_LOCAL_REF.get().get(tenantId);
    }

    static void cleanUp() {
        TENANT_METER_CACHE.cleanUp();
    }
}
