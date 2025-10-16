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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionNumGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSessionSpaceGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentSubCountGauge;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.apache.bifromq.metrics.ITenantMeter;

class TenantStats {
    private final LongAdder sessionCount = new LongAdder();
    private final LongAdder subCount = new LongAdder();

    private final String tenantId;
    private final String[] tags;

    private boolean isLeader;

    TenantStats(String tenantId, Supplier<Number> usedSpaceGetter, String... tagValuePair) {
        this.tenantId = tenantId;
        this.tags = tagValuePair;
        ITenantMeter.gauging(tenantId, MqttPersistentSessionSpaceGauge, usedSpaceGetter, tags);
    }

    public void addSessionCount(int delta) {
        sessionCount.add(delta);
    }

    public void addSubCount(int delta) {
        subCount.add(delta);
    }

    boolean isNoSession() {
        return sessionCount.sum() == 0;
    }

    void toggleMetering(boolean isLeader) {
        if (!this.isLeader && isLeader) {
            ITenantMeter.gauging(tenantId, MqttPersistentSubCountGauge, subCount::sum, tags);
            ITenantMeter.gauging(tenantId, MqttPersistentSessionNumGauge, sessionCount::sum, tags);
            this.isLeader = true;
        } else if (this.isLeader && !isLeader) {
            ITenantMeter.stopGauging(tenantId, MqttPersistentSubCountGauge, tags);
            ITenantMeter.stopGauging(tenantId, MqttPersistentSessionNumGauge, tags);
            this.isLeader = false;
        }
    }

    void destroy() {
        ITenantMeter.stopGauging(tenantId, MqttPersistentSubCountGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttPersistentSessionNumGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttPersistentSessionSpaceGauge, tags);
    }
}
