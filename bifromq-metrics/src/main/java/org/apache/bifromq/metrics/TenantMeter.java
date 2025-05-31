/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.lang.ref.Cleaner;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TenantMeter implements ITenantMeter {
    private static final Cleaner CLEANER = Cleaner.create();

    private static class State implements Runnable {
        final Map<TenantMetric, Meter> meters = new HashMap<>();

        State(Tags tags) {
            for (TenantMetric metric : TenantMetric.values()) {
                switch (metric.meterType) {
                    case COUNTER:
                        meters.put(metric, Metrics.counter(metric.metricName, tags));
                        break;
                    case TIMER:
                        meters.put(metric, Metrics.timer(metric.metricName, tags));
                        break;
                    case DISTRIBUTION_SUMMARY:
                        meters.put(metric, Metrics.summary(metric.metricName, tags));
                        break;
                    case GAUGE:
                        // ignore gauge
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported traffic meter type");
                }
            }
        }

        @Override
        public void run() {
            meters.values().forEach(Metrics.globalRegistry::remove);
            meters.clear();
        }
    }

    private final State state;
    private final Tags tags;
    private final Cleaner.Cleanable cleanable;

    public TenantMeter(String tenantId) {
        this.tags = Tags.of(TAG_TENANT_ID, tenantId);
        this.state = new State(tags);
        this.cleanable = CLEANER.register(this, state);
    }

    @Override
    public void recordCount(TenantMetric metric) {
        recordCount(metric, 1);
    }

    @Override
    public void recordCount(TenantMetric metric, double inc) {
        assert metric.meterType == Meter.Type.COUNTER;
        ((Counter) state.meters.get(metric)).increment(inc);
    }

    @Override
    public Timer timer(TenantMetric metric) {
        assert metric.meterType == Meter.Type.TIMER;
        return (Timer) state.meters.get(metric);
    }

    @Override
    public void recordSummary(TenantMetric metric, double value) {
        assert metric.meterType == Meter.Type.DISTRIBUTION_SUMMARY;
        ((DistributionSummary) state.meters.get(metric)).record(value);
    }


    public void destroy() {
        cleanable.clean();
    }
}
