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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.metrics.TenantMetric.MqttRouteSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttSharedSubNumGauge;

import org.apache.bifromq.metrics.ITenantMeter;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

class TenantRouteState {
    private final LongAdder normalRoutes = new LongAdder();
    private final LongAdder sharedRoutes = new LongAdder();
    private final String tenantId;
    private final String[] tags;

    TenantRouteState(String tenantId, Supplier<Number> spaceUsageProvider, String... tags) {
        this.tenantId = tenantId;
        this.tags = tags;
        ITenantMeter.gauging(tenantId, MqttRouteSpaceGauge, spaceUsageProvider, tags);
        ITenantMeter.gauging(tenantId, MqttSharedSubNumGauge, sharedRoutes::sum, tags);
    }

    void addNormalRoutes(int delta) {
        normalRoutes.add(delta);
    }

    void addSharedRoutes(int delta) {
        sharedRoutes.add(delta);
    }

    boolean isNoRoutes() {
        return normalRoutes.sum() == 0 && sharedRoutes.sum() == 0;
    }

    void destroy() {
        ITenantMeter.stopGauging(tenantId, MqttRouteSpaceGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttSharedSubNumGauge, tags);
    }
}
