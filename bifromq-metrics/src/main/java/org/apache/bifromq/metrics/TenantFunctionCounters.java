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

package org.apache.bifromq.metrics;

import static org.apache.bifromq.metrics.ITenantMeter.TAG_TENANT_ID;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.ToDoubleFunction;

class TenantFunctionCounters {
    private static final ConcurrentMap<String, Map<TenantMetricKey, FunctionCounter>> TENANT_FUNCTION_COUNTERS = new ConcurrentHashMap<>();

    static <C> void counting(String tenantId, TenantMetric counterMetric, C ctr, ToDoubleFunction<C> supplier,
                             String... tagValuePair) {
        assert counterMetric.meterType == Meter.Type.COUNTER && counterMetric.isFunction;
        TENANT_FUNCTION_COUNTERS.compute(tenantId, (k, v) -> {
            if (v == null) {
                v = new HashMap<>();
            }
            Tags tags = Tags.of(tagValuePair);
            v.computeIfAbsent(new TenantMetricKey(counterMetric, tags),
                tenantMetricKey -> FunctionCounter.builder(counterMetric.metricName, ctr, supplier)
                    .tags(tags.and(TAG_TENANT_ID, tenantId))
                    .register(Metrics.globalRegistry));
            return v;
        });
    }

    static void stopCounting(String tenantId, TenantMetric counterMetric, String... tagValuePair) {
        assert counterMetric.meterType == Meter.Type.COUNTER && counterMetric.isFunction;
        TENANT_FUNCTION_COUNTERS.computeIfPresent(tenantId, (k, ctrMap) -> {
            Tags tags = Tags.of(tagValuePair);
            FunctionCounter functionCounter = ctrMap.remove(new TenantMetricKey(counterMetric, tags));
            if (functionCounter != null) {
                Metrics.globalRegistry.remove(functionCounter);
            }
            if (ctrMap.isEmpty()) {
                return null;
            }
            return ctrMap;
        });
    }

    private record TenantMetricKey(TenantMetric counterMetrics, Tags tags) {
    }
}
