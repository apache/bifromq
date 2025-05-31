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

import org.apache.bifromq.baserpc.metrics.RPCMetric;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AckStream<AckT, MsgT> extends AbstractStreamObserver<AckT, MsgT> {
    private final PublishSubject<AckT> ackSubject = PublishSubject.create();
    private final AtomicBoolean closed = new AtomicBoolean();

    protected AckStream(StreamObserver<MsgT> responseObserver) {
        super(responseObserver);
    }

    public Observable<AckT> ack() {
        return ackSubject;
    }

    public void send(MsgT message) {
        responseObserver.onNext(message);
        meter.recordCount(RPCMetric.StreamMsgSendCount);
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            ackSubject.onComplete();
            if (!responseObserver.isCancelled()) {
                responseObserver.onCompleted();
            }
        }
    }

    @Override
    public final void onNext(AckT value) {
        meter.recordCount(RPCMetric.StreamAckReceiveCount);
        ackSubject.onNext(value);
    }

    @Override
    public final void onError(Throwable t) {
        close();
    }

    @Override
    public final void onCompleted() {
        close();
    }
}
