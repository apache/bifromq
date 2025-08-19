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

package org.apache.bifromq.baserpc.client;

import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Channel;
import io.grpc.ConnectivityState;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.netty.InProcAware;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Builder;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.baseenv.NettyEnv;
import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.client.interceptor.TenantAwareClientInterceptor;
import org.apache.bifromq.baserpc.client.loadbalancer.IServerSelector;
import org.apache.bifromq.baserpc.client.loadbalancer.TrafficDirectiveLoadBalancerProvider;
import org.apache.bifromq.baserpc.client.nameresolver.TrafficGovernorNameResolverProvider;
import org.apache.bifromq.baserpc.client.util.FastBehaviorSubject;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceLandscape;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;

class ClientChannel implements IClientChannel {
    private final String serviceUniqueName;
    private final ExecutorService executorService;
    private final ManagedChannel internalChannel;
    private final FastBehaviorSubject<IServerSelector> serverSelectorSubject = FastBehaviorSubject.create();
    private final FastBehaviorSubject<IRPCClient.ConnState> connStateSubject = FastBehaviorSubject.create();
    // key: server id, value: server attributes
    private final Observable<Map<String, Map<String, String>>> serverListSubject;
    private final LoadBalancerProvider loadBalancerProvider;

    @Builder
    ClientChannel(int workerThreads,
                  long keepAliveInSec,
                  long idleTimeoutInSec,
                  BluePrint bluePrint,
                  IRPCServiceTrafficService trafficService,
                  EventLoopGroup eventLoopGroup,
                  SslContext sslContext) {
        serviceUniqueName = bluePrint.serviceDescriptor().getName();
        IRPCServiceLandscape serviceLandscape = trafficService.getServiceLandscape(serviceUniqueName);
        loadBalancerProvider =
            new TrafficDirectiveLoadBalancerProvider(bluePrint, serverSelectorSubject::onNext);
        serverListSubject = serviceLandscape.serverEndpoints()
            .map(sl -> sl.stream().collect(Collectors.toMap(s -> s.id(), s -> s.attrs())));

        LoadBalancerRegistry.getDefaultRegistry().register(loadBalancerProvider);

        TrafficGovernorNameResolverProvider.register(serviceUniqueName, serviceLandscape);
        if (workerThreads == 0) {
            executorService = MoreExecutors.newDirectExecutorService();
        } else {
            executorService = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
                new ThreadPoolExecutor(workerThreads, workerThreads, 0L,
                    TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                    EnvProvider.INSTANCE.newThreadFactory(serviceUniqueName + "-client-executor")),
                serviceUniqueName + "-rpc-client-executor");
        }
        String target = TrafficGovernorNameResolverProvider.SCHEME + "://" + serviceUniqueName;
        NettyChannelBuilder internalChannelBuilder = NettyChannelBuilder
            .forTarget(TrafficGovernorNameResolverProvider.SCHEME + "://" + serviceUniqueName)
            .keepAliveTime(keepAliveInSec <= 0 ? 600 : keepAliveInSec, TimeUnit.SECONDS)
            .keepAliveWithoutCalls(true)
            .maxInboundMessageSize(Integer.MAX_VALUE);
        if (sslContext != null) {
            internalChannelBuilder
                .negotiationType(NegotiationType.TLS)
                .sslContext(sslContext);
        } else {
            internalChannelBuilder
                .negotiationType(NegotiationType.PLAINTEXT);
        }
        if (eventLoopGroup != null) {
            internalChannelBuilder.eventLoopGroup(eventLoopGroup)
                .channelType(NettyEnv.determineSocketChannelClass(eventLoopGroup));
        }

        internalChannel = InProcAware.wrap(target, internalChannelBuilder)
            .idleTimeout(idleTimeoutInSec <= 0 ? (365 * 24 * 3600) : idleTimeoutInSec, TimeUnit.SECONDS)
            .defaultLoadBalancingPolicy(loadBalancerProvider.getPolicyName())
            .intercept(new TenantAwareClientInterceptor())
            .executor(executorService)
            .build();
        ConnStateListener connStateListener = (server, connState) ->
            connStateSubject.onNext(IRPCClient.ConnState.values()[connState.ordinal()]);
        startStateListener(connStateListener);
    }


    @Override
    public Channel channel() {
        return internalChannel;
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return connStateSubject;
    }

    @Override
    public Observable<Map<String, Map<String, String>>> serverList() {
        return serverListSubject;
    }

    @Override
    public Observable<IServerSelector> serverSelectorObservable() {
        return serverSelectorSubject.distinctUntilChanged();
    }

    @Override
    public boolean shutdown(long timeout, TimeUnit unit) {
        if (internalChannel.isShutdown()) {
            return true;
        }
        LoadBalancerRegistry.getDefaultRegistry().deregister(loadBalancerProvider);
        boolean result;
        long start = System.nanoTime();
        try {
            internalChannel.shutdownNow();
            result = internalChannel.awaitTermination(timeout / 2, unit);
        } catch (InterruptedException e) {
            result = internalChannel.isTerminated();
        }
        long left = timeout - unit.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        MoreExecutors.shutdownAndAwaitTermination(executorService, Math.max(1, left), unit);
        serverSelectorSubject.onComplete();
        connStateSubject.onComplete();
        return result;
    }

    private void startStateListener(ConnStateListener connStateListener) {
        ConnectivityState currentState = this.internalChannel.getState(true);
        connStateListener.onChange(serviceUniqueName, currentState);
        if (currentState != ConnectivityState.SHUTDOWN) {
            this.internalChannel.notifyWhenStateChanged(currentState,
                () -> startStateListener(connStateListener));
        }
    }
}
