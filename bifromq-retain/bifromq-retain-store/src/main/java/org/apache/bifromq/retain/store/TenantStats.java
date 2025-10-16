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

package org.apache.bifromq.retain.store;

import static org.apache.bifromq.metrics.TenantMetric.MqttRetainNumGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttRetainSpaceGauge;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.bifromq.metrics.ITenantMeter;

class TenantStats {
    private final AtomicLong topicCount = new AtomicLong();
    private final String tenantId;
    private final String[] tags;
    private boolean isLeader;

    TenantStats(String tenantId, Supplier<Number> usedSpaceGetter, String... tags) {
        this.tenantId = tenantId;
        this.tags = tags;
        ITenantMeter.gauging(tenantId, MqttRetainSpaceGauge, usedSpaceGetter, tags);
    }

    public long incrementTopicCount(int delta) {
        return topicCount.addAndGet(delta);
    }

    void toggleMetering(boolean isLeader) {
        if (!this.isLeader && isLeader) {
            ITenantMeter.gauging(tenantId, MqttRetainNumGauge, topicCount::get, tags);
            this.isLeader = true;
        } else if (this.isLeader && !isLeader) {
            ITenantMeter.stopGauging(tenantId, MqttRetainNumGauge, tags);
            this.isLeader = false;
        }
    }

    void destroy() {
        ITenantMeter.stopGauging(tenantId, MqttRetainSpaceGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttRetainNumGauge, tags);
    }
}
