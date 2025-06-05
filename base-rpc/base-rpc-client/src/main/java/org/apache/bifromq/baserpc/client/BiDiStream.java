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

import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;

import org.apache.bifromq.baserpc.RPCContext;
import org.apache.bifromq.baserpc.client.util.FastBehaviorSubject;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BiDiStream<InT, OutT> implements IBiDiStream<InT, OutT> {
    private final String tenantId;
    private final String serverId;
    private final ClientCallStreamObserver<InT> callStreamObserver;
    private final Subject<OutT> outSubject = PublishSubject.create();
    private final Subject<Long> onReadySubject = FastBehaviorSubject.create();
    private final AtomicBoolean isCancelled = new AtomicBoolean();
    private final AtomicBoolean isClosed = new AtomicBoolean();

    @SneakyThrows
    BiDiStream(String tenantId,
               String serverId,
               Channel channel,
               MethodDescriptor<InT, OutT> methodDescriptor,
               Map<String, String> metadata,
               CallOptions callOptions) {
        this.tenantId = tenantId;
        this.serverId = serverId;
        Context ctx = Context.ROOT.fork()
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .withValue(RPCContext.DESIRED_SERVER_ID_CTX_KEY, serverId)
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metadata);
        callStreamObserver = ctx.call(() -> (ClientCallStreamObserver<InT>)
            asyncBidiStreamingCall(channel.newCall(methodDescriptor, callOptions),
                new ClientResponseObserver<>() {

                    @Override
                    public void beforeStart(ClientCallStreamObserver<Object> requestStream) {
                        requestStream.setOnReadyHandler(() -> onReadySubject.onNext(System.nanoTime()));
                    }

                    @Override
                    public void onNext(OutT outT) {
                        outSubject.onNext(outT);
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.trace("BiDiStream error: serverId={}", serverId, t);
                        outSubject.onError(t);
                        onReadySubject.onComplete();
                    }

                    @Override
                    public void onCompleted() {
                        outSubject.onComplete();
                        onReadySubject.onComplete();
                    }
                }));
    }

    @Override
    public Observable<OutT> onNext() {
        return outSubject;
    }

    @Override
    public Observable<Long> onReady() {
        return onReadySubject;
    }

    @Override
    public String serverId() {
        return serverId;
    }

    public boolean isReady() {
        return callStreamObserver.isReady();
    }

    public void cancel(String message) {
        if (isCancelled.compareAndSet(false, true)) {
            callStreamObserver.cancel(message, null);
        }
    }

    public void send(InT in) {
        try {
            callStreamObserver.onNext(in);
        } catch (Throwable e) {
            log.error("Failed to send message", e);
        }
    }

    public void close() {
        if (isCancelled.get()) {
            return;
        }
        if (isClosed.compareAndSet(false, true)) {
            callStreamObserver.onCompleted();
        }
    }
}
