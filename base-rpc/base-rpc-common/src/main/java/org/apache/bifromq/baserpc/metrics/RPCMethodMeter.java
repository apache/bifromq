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

package org.apache.bifromq.baserpc.metrics;

import org.apache.bifromq.baserpc.BluePrint;
import io.grpc.MethodDescriptor;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.EnumMap;

public class RPCMethodMeter implements IRPCMeter.IRPCMethodMeter {
    private final EnumMap<RPCMetric, Meter> meters;

    public RPCMethodMeter(String serviceName, MethodDescriptor<?, ?> methodDesc, BluePrint.MethodSemantic semantic) {
        meters = new EnumMap<>(RPCMetric.class);
        for (RPCMetric metric : RPCMetric.values()) {
            if (metric.methodType != semantic.type()) {
                continue;
            }
            Tags tags = Tags.of(MetricTag.SERVICE, serviceName)
                .and(MetricTag.METHOD, methodDesc.getBareMethodName() == null ?
                    methodDesc.getFullMethodName() : methodDesc.getBareMethodName());
            meters.put(metric, switch (metric.meterType) {
                case TIMER -> Timer.builder(metric.metricName)
                    .tags(tags)
                    .register(Metrics.globalRegistry);
                case COUNTER -> Metrics.counter(metric.metricName, tags);
                case DISTRIBUTION_SUMMARY -> DistributionSummary.builder(metric.metricName)
                    .tags(tags)
                    .register(Metrics.globalRegistry);
                default -> throw new IllegalStateException("Unsupported Meter Type: " + metric.meterType);
            });
        }
    }

    @Override
    public void recordCount(RPCMetric metric) {
        assert metric.meterType == Meter.Type.COUNTER;
        ((Counter) meters.get(metric)).increment();
    }

    @Override
    public void recordCount(RPCMetric metric, double inc) {
        assert metric.meterType == Meter.Type.COUNTER;
        ((Counter) meters.get(metric)).increment(inc);

    }

    @Override
    public Timer timer(RPCMetric metric) {
        assert metric.meterType == Meter.Type.TIMER;
        return (Timer) meters.get(metric);

    }

    @Override
    public void recordSummary(RPCMetric metric, int depth) {
        assert metric.meterType == Meter.Type.DISTRIBUTION_SUMMARY;
        ((DistributionSummary) meters.get(metric)).record(depth);
    }
}
