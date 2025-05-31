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

package org.apache.bifromq.baserpc.server;

import org.apache.bifromq.baseenv.EnvProvider;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ResponsePipeline<RequestT, ResponseT> extends AbstractResponsePipeline<RequestT, ResponseT> {
    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(
        EnvProvider.INSTANCE.newThreadFactory("base-rpc-flow-controller", true));
    private final Supplier<Boolean> flowControlSignal;
    private final AtomicBoolean flowControlEnabled = new AtomicBoolean(false);
    private final long timeoutNanos;
    private final AtomicLong throttledAt = new AtomicLong(Long.MAX_VALUE);

    public ResponsePipeline(StreamObserver<ResponseT> responseObserver) {
        this(responseObserver, null, null);
    }

    public ResponsePipeline(StreamObserver<ResponseT> respObserver,
                            Supplier<Boolean> slowDownSignal,
                            Duration slowDownTimeout) {
        super(respObserver);
        if (slowDownSignal != null) {
            this.flowControlSignal = slowDownSignal;
            this.responseObserver.disableAutoRequest();
            this.responseObserver.setOnReadyHandler(new Runnable() {
                // ensure that the first request is sent immediately
                private boolean wasReady = false;

                @Override
                public void run() {
                    if (responseObserver.isReady() && !wasReady) {
                        wasReady = true;
                        responseObserver.request(1);
                    }
                }
            });
            this.timeoutNanos = slowDownTimeout.toNanos();
        } else {
            // no flow control
            this.flowControlSignal = () -> false;
            this.timeoutNanos = 0;
        }
    }

    @Override
    public final void onNext(RequestT request) {
        startHandlingRequest(request).thenAccept((response) -> emitResponse(request, response));
        if (!flowControlEnabled.get()) {
            if (flowControlSignal.get() && flowControlEnabled.compareAndSet(false, true)) {
                throttledAt.set(System.nanoTime());
                log.debug("ResponsePipeline@{} flow control start", this.hashCode());
                checkSignal();
            } else {
                // flow control is disabled and no signal, request more
                responseObserver.request(1);
            }
        }
    }

    private void checkSignal() {
        SCHEDULER.schedule(() -> {
            if (!flowControlSignal.get()) {
                throttledAt.set(Long.MAX_VALUE);
                flowControlEnabled.set(false);
                log.debug("ResponsePipeline@{} flow control stop", this.hashCode());
                responseObserver.request(1);
            } else {
                if (System.nanoTime() - throttledAt.get() > timeoutNanos) {
                    log.debug("ResponsePipeline@{} flow control timeout", this.hashCode());
                    responseObserver.onError(Status.RESOURCE_EXHAUSTED.asRuntimeException());
                } else {
                    checkSignal();
                }
            }
        }, 100, TimeUnit.MILLISECONDS);
    }
}
