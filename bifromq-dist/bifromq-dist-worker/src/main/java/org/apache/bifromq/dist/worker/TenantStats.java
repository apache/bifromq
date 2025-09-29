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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.metrics.TenantMetric.MqttRouteNumGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttRouteSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttSharedSubNumGauge;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.apache.bifromq.metrics.ITenantMeter;

class TenantStats {
    private final LongAdder normalRoutes = new LongAdder();
    private final LongAdder sharedRoutes = new LongAdder();
    private final String tenantId;
    private final String[] tags;
    private boolean isLeader;

    TenantStats(String tenantId, Supplier<Number> spaceUsageProvider, String... tags) {
        this.tenantId = tenantId;
        this.tags = tags;
        ITenantMeter.gauging(tenantId, MqttRouteSpaceGauge, spaceUsageProvider, tags);
    }

    void addNormalRoutes(int delta) {
        normalRoutes.add(delta);
        toggleMetering(isLeader);
    }

    void addSharedRoutes(int delta) {
        sharedRoutes.add(delta);
        toggleMetering(isLeader);
    }

    boolean isNoRoutes() {
        return normalRoutes.sum() == 0 && sharedRoutes.sum() == 0;
    }

    void toggleMetering(boolean isLeader) {
        if (!this.isLeader && isLeader) {
            ITenantMeter.gauging(tenantId, MqttRouteNumGauge, normalRoutes::sum, tags);
            ITenantMeter.gauging(tenantId, MqttSharedSubNumGauge, sharedRoutes::sum, tags);
            this.isLeader = true;
        } else if (this.isLeader && !isLeader) {
            ITenantMeter.stopGauging(tenantId, MqttRouteNumGauge, tags);
            ITenantMeter.stopGauging(tenantId, MqttSharedSubNumGauge, tags);
            this.isLeader = false;
        }
    }

    void destroy() {
        ITenantMeter.stopGauging(tenantId, MqttRouteSpaceGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttRouteNumGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttSharedSubNumGauge, tags);
    }
}
