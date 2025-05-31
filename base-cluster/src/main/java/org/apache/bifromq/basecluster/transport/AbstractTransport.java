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

package org.apache.bifromq.basecluster.transport;

import org.apache.bifromq.basecluster.transport.proto.Packet;
import org.apache.bifromq.basehlc.HLC;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class AbstractTransport implements ITransport {
    private final String env;
    private final Subject<PacketEnvelope> publisher = PublishSubject.<PacketEnvelope>create();
    private final AtomicBoolean hasStopped = new AtomicBoolean();

    @Override
    public abstract InetSocketAddress bindAddress();

    @Override
    public final CompletableFuture<Void> send(List<ByteString> data, InetSocketAddress recipient) {
        checkState();
        // piggyback local basehlc
        Packet.Builder builder = Packet.newBuilder().setHlc(HLC.INST.get()).addAllMessages(data);
        if (!Strings.isNullOrEmpty(env)) {
            builder.setClusterEnv(env);
        }
        Packet packet = builder.build();
        return doSend(packet, recipient);
    }

    @Override
    public final Observable<PacketEnvelope> receive() {
        checkState();
        return publisher;
    }

    @Override
    public final CompletableFuture<Void> shutdown() {
        if (hasStopped.compareAndSet(false, true)) {
            CompletableFuture<Void> onDone = new CompletableFuture<>();
            Completable.concatArrayDelayError(doShutdown(), Completable.fromAction(publisher::onComplete))
                .onErrorComplete()
                .subscribe(() -> onDone.complete(null));
            return onDone;
        }
        return CompletableFuture.completedFuture(null);
    }

    protected void doReceive(Packet packet, InetSocketAddress sender, InetSocketAddress recipient) {
        if (!Strings.isNullOrEmpty(env)
            && !Strings.isNullOrEmpty(packet.getClusterEnv())
            && !env.equals(packet.getClusterEnv())
        ) {
            return;
        }
        synchronized (publisher) {
            // update local basehlc before deliver to app logic
            HLC.INST.update(packet.getHlc());
            publisher.onNext(new PacketEnvelope(packet.getMessagesList(), recipient, sender));
        }
    }

    protected abstract CompletableFuture<Void> doSend(Packet packet, InetSocketAddress recipient);

    protected abstract Completable doShutdown();

    private void checkState() {
        Preconditions.checkState(!hasStopped.get(), "Has stopped");
    }
}
