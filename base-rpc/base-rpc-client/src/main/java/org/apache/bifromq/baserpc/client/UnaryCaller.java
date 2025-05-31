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

package org.apache.bifromq.baserpc.client;

import static org.apache.bifromq.baserpc.RPCContext.DESIRED_SERVER_ID_CTX_KEY;
import static org.apache.bifromq.baserpc.RPCContext.TENANT_ID_CTX_KEY;
import static org.apache.bifromq.baserpc.client.exception.ExceptionUtil.toConcreteException;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;

import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.baserpc.client.exception.ServiceUnavailableException;
import org.apache.bifromq.baserpc.client.loadbalancer.IServerGroupRouter;
import org.apache.bifromq.baserpc.client.loadbalancer.IServerSelector;
import org.apache.bifromq.baserpc.metrics.IRPCMeter;
import org.apache.bifromq.baserpc.metrics.RPCMetric;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class UnaryCaller<ReqT, RespT> implements IUnaryCaller<ReqT, RespT> {
    private final Supplier<IServerSelector> serverSelectorSupplier;
    private final Channel channel;
    private final MethodDescriptor<ReqT, RespT> methodDesc;
    private final BluePrint.MethodSemantic semantic;
    private final CallOptions callOptions;
    private final IRPCMeter.IRPCMethodMeter meter;
    private final AtomicInteger counter;

    UnaryCaller(Supplier<IServerSelector> serverSelectorSupplier,
                Channel channel,
                CallOptions callOptions,
                MethodDescriptor<ReqT, RespT> methodDesc,
                BluePrint bluePrint,
                IRPCMeter.IRPCMethodMeter meter,
                AtomicInteger counter) {
        assert methodDesc.getType() == MethodDescriptor.MethodType.UNARY;
        this.semantic = bluePrint.semantic(methodDesc.getFullMethodName());
        assert semantic instanceof BluePrint.Unary;
        this.serverSelectorSupplier = serverSelectorSupplier;
        this.channel = channel;
        this.methodDesc = methodDesc;
        this.callOptions = callOptions;
        this.meter = meter;
        this.counter = counter;
    }

    @SneakyThrows
    @Override
    public CompletableFuture<RespT> invoke(String tenantId,
                                           @Nullable String targetServerId,
                                           ReqT req,
                                           Map<String, String> metadata) {
        IServerSelector serverSelector = serverSelectorSupplier.get();
        IServerGroupRouter router = serverSelector.get(tenantId);
        String finalServerId;
        switch (semantic.mode()) {
            case DDBalanced -> {
                assert targetServerId != null;
                if (serverSelector.exists(targetServerId)) {
                    finalServerId = targetServerId;
                } else {
                    return CompletableFuture.failedFuture(
                        new ServerNotFoundException("Server not found: " + targetServerId));
                }
            }
            case WCHBalanced -> {
                assert semantic instanceof BluePrint.WCHBalancedReq;
                @SuppressWarnings("unchecked")
                String wchKey = ((BluePrint.WCHBalancedReq<ReqT>) semantic).hashKey(req);
                assert wchKey != null;
                Optional<String> selectedServerId = router.hashing(wchKey);
                if (selectedServerId.isPresent()) {
                    finalServerId = selectedServerId.get();
                } else {
                    return CompletableFuture.failedFuture(
                        new ServiceUnavailableException("Service unavailable for " + methodDesc.getFullMethodName()));
                }
            }
            case WRBalanced -> {
                assert semantic instanceof BluePrint.WRUnaryMethod;
                Optional<String> selectedServerId = router.random();
                if (selectedServerId.isPresent()) {
                    finalServerId = selectedServerId.get();
                } else {
                    return CompletableFuture.failedFuture(
                        new ServiceUnavailableException("Service unavailable for " + methodDesc.getFullMethodName()));
                }
            }
            case WRRBalanced -> {
                assert semantic instanceof BluePrint.WRRUnaryMethod;
                Optional<String> selectedServerId = router.roundRobin();
                if (selectedServerId.isPresent()) {
                    finalServerId = selectedServerId.get();
                } else {
                    return CompletableFuture.failedFuture(
                        new ServiceUnavailableException("Service unavailable for " + methodDesc.getFullMethodName()));
                }
            }
            default -> {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("Unknown balance mode: " + semantic.mode()));
            }
        }
        Context ctx = Context.ROOT.fork().withValue(TENANT_ID_CTX_KEY, tenantId)
            .withValue(DESIRED_SERVER_ID_CTX_KEY, finalServerId);
        return ctx.call(() -> {
            long startNano = System.nanoTime();
            CompletableFuture<RespT> future = new CompletableFuture<>();
            int currentCount = counter.incrementAndGet();
            meter.recordCount(RPCMetric.UnaryReqSendCount);
            meter.recordSummary(RPCMetric.UnaryReqDepth, currentCount);
            asyncUnaryCall(channel.newCall(methodDesc, callOptions), req,
                new StreamObserver<>() {
                    @Override
                    public void onNext(RespT resp) {
                        long l = System.nanoTime() - startNano;
                        meter.timer(RPCMetric.UnaryReqLatency).record(l, TimeUnit.NANOSECONDS);
                        future.complete(resp);
                        meter.recordCount(RPCMetric.UnaryReqCompleteCount);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        log.debug("Unary call of method {} error:", methodDesc.getFullMethodName(), throwable);
                        future.completeExceptionally(toConcreteException(throwable));
                        meter.recordCount(RPCMetric.UnaryReqAbortCount);
                    }

                    @Override
                    public void onCompleted() {
                        counter.decrementAndGet();
                        // do nothing
                        meter.recordSummary(RPCMetric.UnaryReqDepth, counter.get());
                    }
                });
            return future;
        });
    }
}
