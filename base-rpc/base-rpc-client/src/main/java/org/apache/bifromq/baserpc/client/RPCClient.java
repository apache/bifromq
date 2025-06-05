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

import com.google.common.collect.Maps;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.client.loadbalancer.IServerSelector;
import org.apache.bifromq.baserpc.metrics.RPCMeter;

@Slf4j
final class RPCClient implements IRPCClient {
    private final BluePrint bluePrint;
    private final IClientChannel channelHolder;
    private final CallOptions defaultCallOptions;
    private final RPCMeter meter;
    private final Map<String, IUnaryCaller<?, ?>> unaryCallers = Maps.newHashMap();
    private final Map<String, AtomicInteger> unaryInflightCounts = Maps.newHashMap();
    private final Disposable disposable;
    private volatile IServerSelector serverSelector = DummyServerSelector.INSTANCE;

    RPCClient(@NonNull BluePrint bluePrint,
              @NonNull IClientChannel channelHolder) {
        this.channelHolder = channelHolder;
        this.bluePrint = bluePrint;
        this.meter = new RPCMeter(bluePrint.serviceDescriptor(), bluePrint);
        this.defaultCallOptions = CallOptions.DEFAULT;
        for (String fullMethodName : bluePrint.allMethods()) {
            if (bluePrint.semantic(fullMethodName) instanceof BluePrint.Unary) {
                MethodDescriptor<?, ?> methodDesc = bluePrint.methodDesc(fullMethodName);
                unaryInflightCounts.put(fullMethodName, new AtomicInteger());
                unaryCallers.put(fullMethodName, new UnaryCaller<>(
                    () -> serverSelector,
                    channelHolder.channel(),
                    defaultCallOptions,
                    methodDesc,
                    bluePrint,
                    meter.get(methodDesc),
                    unaryInflightCounts.get(methodDesc.getFullMethodName())));
            }
        }
        disposable =
            channelHolder.serverSelectorObservable().subscribe(serverSelector -> this.serverSelector = serverSelector);
    }

    public void stop() {
        disposable.dispose();
        this.channelHolder.shutdown(5, TimeUnit.SECONDS);
    }

    @Override
    public Observable<Map<String, Map<String, String>>> serverList() {
        return channelHolder.serverList();
    }

    @Override
    public Observable<ConnState> connState() {
        return channelHolder.connState();
    }

    public <ReqT, RespT> CompletableFuture<RespT> invoke(String tenantId,
                                                         String desiredServerId,
                                                         ReqT req,
                                                         @NonNull Map<String, String> metadata,
                                                         MethodDescriptor<ReqT, RespT> methodDesc) {
        @SuppressWarnings("unchecked")
        IUnaryCaller<ReqT, RespT> caller = (IUnaryCaller<ReqT, RespT>) unaryCallers.get(methodDesc.getFullMethodName());
        return caller.invoke(tenantId, desiredServerId, req, metadata);
    }

    @Override
    public <ReqT, RespT> IRequestPipeline<ReqT, RespT> createRequestPipeline(String tenantId,
                                                                             String desiredServerId,
                                                                             String wchKey,
                                                                             Supplier<Map<String, String>> metadataSupplier,
                                                                             MethodDescriptor<ReqT, RespT> methodDesc) {
        return new ManagedRequestPipeline<>(
            tenantId,
            wchKey,
            desiredServerId,
            metadataSupplier,
            channelHolder,
            defaultCallOptions,
            methodDesc,
            bluePrint,
            meter.get(methodDesc));
    }

    @Override
    public <MsgT, AckT> IMessageStream<MsgT, AckT> createMessageStream(String tenantId,
                                                                       String desiredServerId,
                                                                       String wchKey,
                                                                       Supplier<Map<String, String>> metadataSupplier,
                                                                       MethodDescriptor<AckT, MsgT> methodDesc) {
        return new ManagedMessageStream<>(
            tenantId,
            wchKey,
            desiredServerId,
            metadataSupplier,
            channelHolder,
            defaultCallOptions,
            methodDesc,
            bluePrint,
            meter.get(methodDesc));
    }
}
