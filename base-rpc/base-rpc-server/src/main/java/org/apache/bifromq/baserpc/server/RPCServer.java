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

package org.apache.bifromq.baserpc.server;

import org.apache.bifromq.baseenv.NettyEnv;
import org.apache.bifromq.baserpc.server.interceptor.TenantAwareServerInterceptor;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceServerRegister;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallExecutorSupplier;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RPCServer implements IRPCServer {
    private final AtomicReference<State>
        state = new AtomicReference<>(State.INIT);
    private final IRPCServiceTrafficService trafficService;
    private final String id;
    private final Map<String, RPCServerBuilder.ServiceDefinition> serviceDefinitions;
    private final Map<String, IRPCServiceServerRegister.IServerRegistration> registrations = new HashMap<>();
    private final EventLoopGroup bossEventLoopGroup;
    private final EventLoopGroup workerEventLoopGroup;
    private final Server inProcServer;
    private final Server interProcServer;

    RPCServer(RPCServerBuilder builder) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(builder.host) && !"0.0.0.0".equals(builder.host),
            "Invalid host");
        Preconditions.checkArgument(!builder.serviceDefinitions.isEmpty(), "No service defined");
        this.id = builder.id;
        this.trafficService = builder.trafficService;
        this.serviceDefinitions = builder.serviceDefinitions;

        ServerBuilder<?> serverBuilder = InProcessServerBuilder.forName(this.id)
            .callExecutor(new ServerCallExecutorSupplier() {
                @Override
                public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata metadata) {
                    return serviceDefinitions.get(call.getMethodDescriptor().getServiceName()).executor();
                }
            });
        bindServiceToServer(serverBuilder);
        inProcServer = serverBuilder.build();

        NettyServerBuilder nettyServerBuilder = NettyServerBuilder
            .forAddress(new InetSocketAddress(builder.host, builder.port))
            .permitKeepAliveWithoutCalls(true)
            .maxInboundMessageSize(Integer.MAX_VALUE)
            .callExecutor(new ServerCallExecutorSupplier() {
                @Override
                public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata metadata) {
                    return serviceDefinitions.get(call.getMethodDescriptor().getServiceName()).executor();
                }
            });
        if (builder.sslContext != null) {
            nettyServerBuilder.sslContext(builder.sslContext);
        }
        bossEventLoopGroup = NettyEnv.createEventLoopGroup(1, "rpc-server-boss-elg");
        new NettyEventExecutorMetrics(bossEventLoopGroup).bindTo(Metrics.globalRegistry);
        workerEventLoopGroup = NettyEnv.createEventLoopGroup(builder.workerThreads, "rpc-server-worker-elg");
        new NettyEventExecutorMetrics(workerEventLoopGroup).bindTo(Metrics.globalRegistry);
        // if null, GRPC managed shared eventloop group will be used
        nettyServerBuilder
            .bossEventLoopGroup(bossEventLoopGroup)
            .workerEventLoopGroup(workerEventLoopGroup)
            .channelType(NettyEnv.determineServerSocketChannelClass(bossEventLoopGroup));
        bindServiceToServer(nettyServerBuilder);
        interProcServer = nettyServerBuilder.build();
    }

    private void bindServiceToServer(ServerBuilder<?> builder) {
        serviceDefinitions.forEach((serviceUniqueName, def) -> {
            ServerServiceDefinition.Builder serverDefBuilder =
                ServerServiceDefinition.builder(def.definition().getServiceDescriptor().getName());
            for (ServerMethodDefinition serverMethodDef : def.definition().getMethods()) {
                MethodDescriptor methodDesc =
                    def.bluePrint().methodDesc(serverMethodDef.getMethodDescriptor().getFullMethodName());
                if (methodDesc != null) {
                    serverDefBuilder.addMethod(methodDesc, serverMethodDef.getServerCallHandler());
                } else {
                    serverDefBuilder.addMethod(serverMethodDef);
                }
            }
            ServerServiceDefinition serviceDef = serverDefBuilder.build();
            builder.addService(ServerInterceptors.intercept(serviceDef,
                new TenantAwareServerInterceptor(serviceDef, def.bluePrint())));
        });
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public final void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                inProcServer.start();
                interProcServer.start();
                InetSocketAddress serverAddr = (InetSocketAddress) interProcServer.getListenSockets().get(0);
                serviceDefinitions.forEach((serviceUniqueName, def) -> {
                    log.debug("Start server register for service: {}", serviceUniqueName);
                    registrations.put(serviceUniqueName, trafficService.getServerRegister(serviceUniqueName)
                        .reg(id, serverAddr, def.defaultGroupTags(), def.attributes()));
                });
                state.set(State.STARTED);
            } catch (IOException e) {
                state.set(State.FATAL_FAILURE);
                throw new IllegalStateException("Unable to start rpc server", e);
            }
        }
    }

    @Override
    public void shutdown() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                registrations.forEach((serviceUniqueName, registration) -> {
                    log.debug("Stop server register for service: {}", serviceUniqueName);
                    registration.stop();
                });
                log.debug("Stopping inter-proc server");
                shutdownInternalServer(interProcServer);
                log.debug("Stopping in-proc server");
                shutdownInternalServer(inProcServer);
                bossEventLoopGroup.shutdownGracefully().sync();
                workerEventLoopGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                state.set(State.STOPPED);
            }
        }
    }

    @SneakyThrows
    private void shutdownInternalServer(Server server) {
        // Start graceful shutdown
        server.shutdown();
        try {
            server.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        server.shutdownNow();
        server.awaitTermination();
    }

    private enum State {
        INIT, STARTING, STARTED, STOPPING, STOPPED, FATAL_FAILURE
    }
}
