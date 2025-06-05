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

import org.apache.bifromq.base.util.FutureTracker;
import org.apache.bifromq.baserpc.metrics.RPCMetric;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractResponsePipeline<RequestT, ResponseT> extends AbstractStreamObserver<RequestT, ResponseT> {
    private final AtomicBoolean closed = new AtomicBoolean();
    // used to keep track of opening response futures returned from handleRequest which associated with current
    // ResponsePipeline
    // it's used to finish those associated futures earlier in the event of ResponsePipeline closed, so that
    // the close ResponsePipeline (and its underlying StreamObserver) could be gc'ed as soon as possible
    private final FutureTracker futureTracker = new FutureTracker();

    protected AbstractResponsePipeline(StreamObserver<ResponseT> responseObserver) {
        super(responseObserver);
    }

    @Override
    public final void onError(Throwable t) {
        if (closed.compareAndSet(false, true)) {
            cleanup();
        }
    }

    @Override
    public final void onCompleted() {
        if (closed.compareAndSet(false, true)) {
            cleanup();
        }
    }

    public final boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        close(null);
    }

    private void close(Throwable t) {
        if (closed.compareAndSet(false, true)) {
            if (t != null) {
                log.debug("Close pipeline@{} by error", hashCode(), t);
                responseObserver.onError(t);
            } else {
                log.trace("Close pipeline@{} normally", hashCode());
                responseObserver.onCompleted();
            }
            cleanup();
        }
    }

    /**
     * Handle the request and return the result via completable future, remember always throw exception asynchronously
     * Returned future complete exceptionally will cause pipeline close
     *
     * @param tenantId the tenantId
     * @param request  the request
     * @return a future of response
     */
    protected abstract CompletableFuture<ResponseT> handleRequest(String tenantId, RequestT request);

    // this method is meant to be used in subclass
    final CompletableFuture<ResponseT> startHandlingRequest(RequestT request) {
        log.trace("Start handling request in pipeline@{}: request={}", hashCode(), request);
        meter.recordCount(RPCMetric.PipelineReqReceivedCount);
        Timer.Sample sample = Timer.start();
        CompletableFuture<ResponseT> respFuture = futureTracker.track(handleRequest(tenantId, request));
        // track current response future until it's completed normally or exceptionally
        respFuture.whenComplete((resp, e) -> {
            sample.stop(meter.timer(RPCMetric.PipelineReqProcessTime));
            // untrack current response future
            if (e != null) {
                meter.recordCount(RPCMetric.PipelineReqFailCount);
                // any handling exception will cause pipeline close
                fail(e);
            } else {
                log.trace("Request handling in pipeline@{}: request={}", hashCode(), request);
                meter.recordCount(RPCMetric.PipelineReqFulfillCount);
            }
        });
        return respFuture;
    }


    final void emitResponse(RequestT req, ResponseT resp) {
        if (!isClosed()) {
            log.trace("Response sent in pipeline@{}: request={}, response={}", hashCode(), req, resp);
            // the sendResponse is guaranteed to run in sequential, no need to synchronize
            responseObserver.onNext(resp);
        } else {
            log.trace("Response dropped due to pipeline@{} has been closed: request={}, response={}",
                hashCode(), req, resp);
        }
    }

    protected void afterClose() {
    }


    private void fail(Throwable throwable) {
        if (!isClosed()) {
            if (throwable instanceof CancellationException) {
                log.trace("Pipeline@{} closed due to request handler being canceled", hashCode(), throwable);
                close(Status.CANCELLED.asRuntimeException());
            } else {
                log.trace("Pipeline@{} closed due to request handler failure", hashCode(), throwable);
                close(Status.UNAVAILABLE.asRuntimeException());
            }
        } else {
            log.trace("Pipeline@{} has been closed", hashCode(), throwable);
        }
    }

    private void cleanup() {
        afterClose();
        // stop all on-going response futures if any to free current ResponsePipeline up
        futureTracker.stop();
    }
}