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

import io.micrometer.core.instrument.Meter;

public enum TenantMetric {
    // connection and session related metrics
    MqttConnectionGauge("mqtt.connection.num.gauge", Meter.Type.GAUGE),
    MqttAuthFailureCount("mqtt.auth.failure.count", Meter.Type.COUNTER),
    MqttConnectCount("mqtt.connect.count", Meter.Type.COUNTER),
    MqttDisconnectCount("mqtt.disconnect.count", Meter.Type.COUNTER),
    MqttSessionWorkingMemoryGauge("mqtt.session.mem.gauge", Meter.Type.GAUGE),
    MqttPersistentSessionSpaceGauge("mqtt.psession.space.gauge", Meter.Type.GAUGE),
    MqttLivePersistentSessionGauge("mqtt.psession.live.num.gauge", Meter.Type.GAUGE),
    MqttPersistentSessionNumGauge("mqtt.psession.num.gauge", Meter.Type.GAUGE),

    // network throughput related metrics
    MqttIngressBytes("mqtt.ingress.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttEgressBytes("mqtt.egress.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttChannelLatency("mqtt.channel.latency", Meter.Type.TIMER),

    // publish related metrics
    MqttQoS0IngressBytes("mqtt.ingress.qos0.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS0DistBytes("mqtt.dist.qos0.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS1IngressBytes("mqtt.ingress.qos1.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS1DistBytes("mqtt.dist.qos1.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS2IngressBytes("mqtt.ingress.qos2.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS2DistBytes("mqtt.dist.qos2.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS0EgressBytes("mqtt.egress.qos0.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS1EgressBytes("mqtt.egress.qos1.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS1DeliverBytes("mqtt.deliver.qos1.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS2EgressBytes("mqtt.egress.qos2.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS2DeliverBytes("mqtt.deliver.qos2.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttQoS0InternalLatency("mqtt.in.qos0.latency", Meter.Type.TIMER),
    MqttQoS1InternalLatency("mqtt.in.qos1.latency", Meter.Type.TIMER),
    MqttQoS1ExternalLatency("mqtt.ex.qos1.latency", Meter.Type.TIMER),
    MqttQoS2InternalLatency("mqtt.in.qos2.latency", Meter.Type.TIMER),
    MqttQoS2ExternalLatency("mqtt.ex.qos2.latency", Meter.Type.TIMER),
    MqttTransientFanOutBytes("mqtt.tfanout.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttPersistentFanOutBytes("mqtt.pfanout.bytes", Meter.Type.DISTRIBUTION_SUMMARY),

    MqttSendingQuota("mqtt.ex.quota", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttConfirmingMessages("mqtt.ex.confirming", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttResendBytes("mqtt.ex.resend.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttDeDupBytes("mqtt.ex.dedup.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttStalledCount("mqtt.ex.stalled.count", Meter.Type.COUNTER),

    // subscription related metrics
    MqttRouteSpaceGauge("mqtt.route.space.gauge", Meter.Type.GAUGE),
    MqttRouteNumGauge("mqtt.route.num.gauge", Meter.Type.GAUGE),
    MqttSharedSubNumGauge("mqtt.shared.sub.num.gauge", Meter.Type.GAUGE),
    MqttTransientSubCount("mqtt.tsub.count", Meter.Type.COUNTER),
    MqttTransientSubLatency("mqtt.tsub.latency", Meter.Type.TIMER),
    MqttPersistentSubCount("mqtt.psub.count", Meter.Type.COUNTER),
    MqttPersistentSubLatency("mqtt.psub.latency", Meter.Type.TIMER),
    MqttTransientUnsubCount("mqtt.tunsub.count", Meter.Type.COUNTER),
    MqttTransientUnsubLatency("mqtt.tunsub.latency", Meter.Type.TIMER),
    MqttPersistentUnsubCount("mqtt.punsub.count", Meter.Type.COUNTER),
    MqttPersistentUnsubLatency("mqtt.punsub.latency", Meter.Type.TIMER),
    MqttTransientSubCountGauge("mqtt.tsub.num.gauge", Meter.Type.GAUGE),
    MqttPersistentSubCountGauge("mqtt.psub.num.gauge", Meter.Type.GAUGE),

    MqttRouteCacheSize("mqtt.route.cache.size.gauge", Meter.Type.GAUGE),
    MqttRouteCacheHitCount("mqtt.route.cache.hit.count", Meter.Type.COUNTER, true),
    MqttRouteCacheEvictCount("mqtt.route.cache.evict.count", Meter.Type.COUNTER, true),
    MqttRouteCacheMissCount("mqtt.route.cache.miss.count", Meter.Type.COUNTER, true),
    // retain related
    MqttIngressRetainBytes("mqtt.ingress.retain.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttRetainedBytes("mqtt.retained.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttRetainNumGauge("mqtt.retained.num.gauge", Meter.Type.GAUGE),
    MqttRetainMatchCount("mqtt.retain.match.count", Meter.Type.COUNTER),
    MqttRetainMatchedBytes("mqtt.retain.matched.bytes", Meter.Type.DISTRIBUTION_SUMMARY),
    MqttRetainSpaceGauge("mqtt.retain.space.gauge", Meter.Type.GAUGE);

    public final String metricName;
    public final Meter.Type meterType;
    public final boolean isFunction;

    TenantMetric(String metricName, Meter.Type meterType) {
        this(metricName, meterType, false);
    }

    TenantMetric(String metricName, Meter.Type meterType, boolean isFunction) {
        assert !isFunction || meterType == Meter.Type.COUNTER;
        this.metricName = metricName;
        this.meterType = meterType;
        this.isFunction = isFunction;
    }
}
