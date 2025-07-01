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

package org.apache.bifromq.mqtt.session;

import static org.apache.bifromq.metrics.TenantMetric.MqttSessionWorkingMemoryGauge;
import static org.apache.bifromq.metrics.TenantMetric.MqttTransientSubCountGauge;

import com.google.common.base.Ticker;
import io.netty.channel.ChannelHandlerContext;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.base.util.FutureTracker;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.mqtt.service.ILocalDistService;
import org.apache.bifromq.mqtt.service.ILocalSessionRegistry;
import org.apache.bifromq.mqtt.spi.IUserPropsCustomizer;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.clientbalancer.IClientBalancer;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sysprops.props.ControlPlaneMaxBurstLatencyMillis;

@Slf4j
public final class MQTTSessionContext {
    public final long retryTimeoutNanos = Duration.ofMillis(ControlPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos();
    public final ILocalSessionRegistry localSessionRegistry;
    public final ILocalDistService localDistService;
    public final IEventCollector eventCollector;
    public final IResourceThrottler resourceThrottler;
    public final ISettingProvider settingProvider;
    public final IDistClient distClient;
    public final IInboxClient inboxClient;
    public final IRetainClient retainClient;
    public final ISessionDictClient sessionDictClient;
    public final IClientBalancer clientBalancer;
    public final IUserPropsCustomizer userPropsCustomizer;
    public final String serverId;
    private final IAuthProvider authProvider;
    private final Ticker ticker;
    private final FutureTracker futureTracker = new FutureTracker();
    private final TenantGauge tenantTransientSubNumGauge;
    private final TenantGauge tenantMemGauge;

    @Builder
    MQTTSessionContext(String serverId,
                       ILocalSessionRegistry localSessionRegistry,
                       ILocalDistService localDistService,
                       IAuthProvider authProvider,
                       IDistClient distClient,
                       IInboxClient inboxClient,
                       IRetainClient retainClient,
                       ISessionDictClient sessionDictClient,
                       IClientBalancer clientBalancer,
                       IEventCollector eventCollector,
                       IResourceThrottler resourceThrottler,
                       ISettingProvider settingProvider,
                       IUserPropsCustomizer userPropsCustomizer,
                       Ticker ticker) {
        this.serverId = serverId;
        this.localSessionRegistry = localSessionRegistry;
        this.localDistService = localDistService;
        this.authProvider = authProvider;
        this.eventCollector = eventCollector;
        this.resourceThrottler = resourceThrottler;
        this.settingProvider = settingProvider;
        this.userPropsCustomizer = userPropsCustomizer;
        this.distClient = distClient;
        this.inboxClient = inboxClient;
        this.retainClient = retainClient;
        this.sessionDictClient = sessionDictClient;
        this.clientBalancer = clientBalancer;
        this.ticker = ticker == null ? Ticker.systemTicker() : ticker;
        this.tenantTransientSubNumGauge = new TenantGauge(MqttTransientSubCountGauge);
        this.tenantMemGauge = new TenantGauge(MqttSessionWorkingMemoryGauge);
    }

    public long nanoTime() {
        return ticker.read();
    }

    public IAuthProvider authProvider(ChannelHandlerContext ctx) {
        // a wrapper to ensure async fifo semantic for check call
        return new MQTTSessionAuthProvider(authProvider, ctx);
    }

    public AtomicLong getTransientSubNumGauge(String tenantId) {
        return tenantTransientSubNumGauge.get(tenantId);
    }

    public AtomicLong getSessionMemGauge(String tenantId) {
        return tenantMemGauge.get(tenantId);
    }

    public <T> CompletableFuture<T> trackBgTask(CompletableFuture<T> task) {
        return futureTracker.track(task);
    }

    public CompletableFuture<Void> awaitBgTasksFinish() {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        futureTracker.whenComplete((v, e) -> onDone.complete(null));
        return onDone;
    }
}
