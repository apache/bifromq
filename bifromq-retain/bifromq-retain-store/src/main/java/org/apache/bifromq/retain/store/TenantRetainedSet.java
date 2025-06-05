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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.metrics.TenantMetric.MqttRetainNumGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttRetainSpaceGauge;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.tenantBeginKey;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.metrics.ITenantMeter;
import com.google.protobuf.ByteString;
import java.util.concurrent.atomic.AtomicLong;

public class TenantRetainedSet {
    private final AtomicLong topicCount = new AtomicLong();
    private final String tenantId;
    private final String[] tags;

    public TenantRetainedSet(String tenantId, IKVReader reader, String... tags) {
        this.tenantId = tenantId;
        this.tags = tags;
        ITenantMeter.gauging(tenantId, MqttRetainSpaceGauge, () -> {
            ByteString tenantBeginKey = tenantBeginKey(tenantId);
            Boundary tenantBoundary =
                intersect(toBoundary(tenantBeginKey, upperBound(tenantBeginKey)), reader.boundary());
            if (isNULLRange(tenantBoundary)) {
                return 0L;
            }
            return reader.size(tenantBoundary);
        }, tags);
        ITenantMeter.gauging(tenantId, MqttRetainNumGauge, topicCount::get, tags);
    }

    public long incrementTopicCount(int delta) {
        return topicCount.addAndGet(delta);
    }

    void destroy() {
        ITenantMeter.stopGauging(tenantId, MqttRetainSpaceGauge, tags);
        ITenantMeter.stopGauging(tenantId, MqttRetainNumGauge, tags);
    }
}
