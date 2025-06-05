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

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.client.exception.RequestAbortException;
import org.apache.bifromq.baserpc.client.exception.RequestRejectedException;
import org.apache.bifromq.baserpc.client.exception.RequestThrottledException;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.baserpc.client.exception.ServiceUnavailableException;
import org.apache.bifromq.baserpc.metrics.IRPCMeter;
import org.apache.bifromq.baserpc.metrics.RPCMetric;

@Slf4j
public class ManagedRequestPipeline<ReqT, RespT> extends ManagedBiDiStream<ReqT, RespT>
    implements IRPCClient.IRequestPipeline<ReqT, RespT> {
    private final ConcurrentLinkedDeque<RequestTask> preflightTaskQueue = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<RequestTask> inflightTaskQueue = new ConcurrentLinkedDeque<>();
    private final AtomicInteger taskCount = new AtomicInteger(0);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean sending = new AtomicBoolean(false);
    private final IRPCMeter.IRPCMethodMeter meter;
    private final MethodDescriptor<ReqT, RespT> methodDescriptor;
    private boolean isRetargeting = false;

    ManagedRequestPipeline(String tenantId,
                           String wchKey,
                           String targetServerId,
                           Supplier<Map<String, String>> metadataSupplier,
                           IClientChannel channelHolder,
                           CallOptions callOptions,
                           MethodDescriptor<ReqT, RespT> methodDescriptor,
                           BluePrint bluePrint,
                           IRPCMeter.IRPCMethodMeter meter) {
        super(tenantId,
            wchKey,
            targetServerId,
            bluePrint.semantic(methodDescriptor.getFullMethodName()).mode(),
            metadataSupplier,
            channelHolder.channel(),
            callOptions,
            bluePrint.methodDesc(methodDescriptor.getFullMethodName()));
        this.meter = meter;
        this.methodDescriptor = methodDescriptor;
        start(channelHolder.serverSelectorObservable());
    }

    @Override
    boolean prepareRetarget() {
        synchronized (this) {
            isRetargeting = true;
            return inflightTaskQueue.isEmpty();
        }
    }

    @Override
    boolean canStartRetarget() {
        synchronized (this) {
            return isRetargeting && inflightTaskQueue.isEmpty();
        }
    }

    @Override
    void onStreamCreated() {
        synchronized (this) {
            isRetargeting = false;
        }
        meter.recordCount(RPCMetric.ReqPipelineCreateCount);
    }

    @Override
    void onStreamReady() {
        sendUntilStreamNotReadyOrNoTask();
    }

    @Override
    void onStreamError(Throwable e) {
        meter.recordCount(RPCMetric.ReqPipelineErrorCount);
        cancelInflightTasks(e);

    }

    @Override
    void onNoServerAvailable() {
        // no server available, abort all requests
        cancelPreflightTasks(new ServerNotFoundException("No Server Available"));
    }

    @Override
    void onServiceUnavailable() {
        // service unavailable, abort all requests
        cancelPreflightTasks(new ServiceUnavailableException("Service unavailable"));
    }

    @Override
    void onReceive(RespT out) {
        RequestTask inflightTask = inflightTaskQueue.poll();
        if (inflightTask == null) {
            log.error("ReqPipeline@{} illegal state: No matching request found for the response: method={}, resp={}",
                this.hashCode(), methodDescriptor.getBareMethodName(), out);
            return;
        }
        inflightTask.finish(out);
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            super.close();
            cancelPreflightTasks(new RequestAbortException("Pipeline has closed"));
        }
    }

    @Override
    public CompletableFuture<RespT> invoke(ReqT req) {
        if (isClosed.get()) {
            return CompletableFuture.failedFuture(new RequestRejectedException("Pipeline has closed"));
        }
        RequestTask newRequest = new RequestTask(req);
        switch (state()) {
            case Init, Normal, PendingRetarget, Retargeting, StreamDisconnect -> {
                int currentCount = taskCount.get();
                log.trace("ReqPipeline@{} enqueue request: method={}, queueSize={}, req={}",
                    this.hashCode(), methodDescriptor.getBareMethodName(), currentCount, req);
                preflightTaskQueue.offer(newRequest);
                if (isClosed.get()) {
                    newRequest.finish(new RequestRejectedException("Pipeline has closed"));
                    return newRequest.future;
                }
                sendUntilStreamNotReadyOrNoTask();
                meter.recordCount(RPCMetric.PipelineReqAcceptCount);
                meter.recordSummary(RPCMetric.ReqPipelineDepth, currentCount);
            }
            case NoServerAvailable -> {
                if (balanceMode == BluePrint.BalanceMode.DDBalanced) {
                    newRequest.finish(new ServerNotFoundException("No server available"));
                } else {
                    newRequest.finish(new ServiceUnavailableException("Service unavailable"));
                }
            }
        }
        return newRequest.future;
    }

    private void sendUntilStreamNotReadyOrNoTask() {
        if (sending.compareAndSet(false, true)) {
            synchronized (this) {
                while (isReady() && !isRetargeting) {
                    Optional<RequestTask> requestTask = prepareForFly();
                    if (requestTask.isPresent()) {
                        // only send non-canceled requests
                        meter.timer(RPCMetric.PipelineReqQueueTime)
                            .record(System.nanoTime() - requestTask.get().enqueueTS, TimeUnit.NANOSECONDS);
                        send(requestTask.get().request);
                        meter.recordCount(RPCMetric.PipelineReqSendCount);
                    } else {
                        break;
                    }
                }
            }
            sending.set(false);
            synchronized (this) {
                if (isReady() && !preflightTaskQueue.isEmpty() && !isRetargeting) {
                    // deal with the spurious notification
                    sendUntilStreamNotReadyOrNoTask();
                }
            }
        }
    }

    private Optional<RequestTask> prepareForFly() {
        RequestTask requestTask = preflightTaskQueue.poll();
        if (requestTask != null) {
            inflightTaskQueue.offer(requestTask);
        }
        return Optional.ofNullable(requestTask);
    }

    private void cancelInflightTasks(Throwable e) {
        synchronized (this) {
            if (inflightTaskQueue.isEmpty() && preflightTaskQueue.isEmpty()) {
                return;
            }
            RequestTask pendingTask;
            int i = 0;
            while ((pendingTask = inflightTaskQueue.poll()) != null) {
                pendingTask.finish(e);
                i++;
            }
            log.debug("ReqPipeline@{} abort {} in-flight requests: method={}", this.hashCode(),
                i, methodDescriptor.getBareMethodName());
        }
    }

    private void cancelPreflightTasks(Throwable e) {
        int i = 0;
        RequestTask pendingTask;
        while ((pendingTask = preflightTaskQueue.poll()) != null) {
            pendingTask.finish(e);
            i++;
        }
        log.debug("ReqPipeline@{} abort {} pre-flight requests: method={}",
            this.hashCode(), i, methodDescriptor.getBareMethodName());
    }

    private class RequestTask {
        final Long enqueueTS = System.nanoTime();
        final ReqT request;
        final CompletableFuture<RespT> future;

        RequestTask(ReqT request) {
            this.request = request;
            taskCount.incrementAndGet();
            this.future = new CompletableFuture<>();
            this.future.whenComplete((v, e) -> taskCount.decrementAndGet());
        }

        public void finish(Throwable throwable) {
            if (future.completeExceptionally(throwable)) {
                log.trace("ReqPipeline@{} finished request with error: method={}, req={}, error={}",
                    ManagedRequestPipeline.this.hashCode(),
                    methodDescriptor.getBareMethodName(),
                    request, throwable.getMessage());
            }
            if (throwable instanceof RequestRejectedException || throwable instanceof RequestThrottledException) {
                meter.recordCount(RPCMetric.PipelineReqDropCount);
            } else if (throwable instanceof RequestAbortException) {
                meter.recordCount(RPCMetric.PipelineReqAbortCount);
            }
        }

        public void finish(RespT resp) {
            long finishTime = System.nanoTime() - enqueueTS;
            meter.timer(RPCMetric.PipelineReqLatency).record(finishTime, TimeUnit.NANOSECONDS);
            if (future.complete(resp)) {
                log.trace("ReqPipeline@{} finished request: method={}, req={}, resp={}, flights={}",
                    ManagedRequestPipeline.this.hashCode(),
                    methodDescriptor.getBareMethodName(), request, resp, taskCount.get());
            }
            meter.recordCount(RPCMetric.PipelineReqCompleteCount);
        }
    }
}
