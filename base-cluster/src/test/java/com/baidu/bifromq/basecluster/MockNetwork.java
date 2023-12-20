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

package com.baidu.bifromq.basecluster;

import com.baidu.bifromq.basecluster.transport.ITransport;
import com.baidu.bifromq.basecluster.transport.PacketEnvelope;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MockNetwork {
    private final Map<InetSocketAddress, MockTransport> endpoints = new ConcurrentHashMap<>();
    // cut src transport to set of dst transports
    private final Map<InetSocketAddress, Set<InetSocketAddress>> cutMap = new ConcurrentHashMap<>();

    public ITransport create() {
        MockTransport transport = new MockTransport();
        endpoints.put(transport.bindAddress(), transport);
        return transport;
    }

    public void isolate(ITransport transport) {
        endpoints.keySet().forEach(peerAddr -> {
            if (!peerAddr.equals(transport.bindAddress())) {
                cutMap.computeIfAbsent(transport.bindAddress(), k -> ConcurrentHashMap.newKeySet()).add(peerAddr);
                cutMap.computeIfAbsent(peerAddr, k -> ConcurrentHashMap.newKeySet()).add(transport.bindAddress());
            }
        });
    }

    public void integrate(ITransport transport) {
        cutMap.remove(transport.bindAddress());
        cutMap.values().forEach(peers -> peers.remove(transport.bindAddress()));
    }

    private class MockTransport implements ITransport {
        private static AtomicInteger nextPort = new AtomicInteger(1);
        private final InetSocketAddress localAddress;
        private final InetSocketAddress listeningAddress;
        private final Subject<PacketEnvelope> channel = PublishSubject.<PacketEnvelope>create().toSerialized();

        private MockTransport() {
            this.localAddress = new InetSocketAddress(nextPort.getAndIncrement());
            this.listeningAddress = new InetSocketAddress("127.0.0.1", nextPort.getAndIncrement());
        }

        @Override
        public InetSocketAddress bindAddress() {
            return listeningAddress;
        }

        @Override
        public CompletableFuture<Void> send(List<ByteString> data, InetSocketAddress recipient) {
            MockTransport peer = endpoints.get(recipient);
            if (peer == null) {
                return CompletableFuture.failedFuture(new UnknownHostException());
            }
            if (cutMap.getOrDefault(listeningAddress, Collections.emptySet()).contains(recipient)) {
                return CompletableFuture.completedFuture(null);
            }
            return peer.inbound(new PacketEnvelope(data, recipient, localAddress));
        }

        @Override
        public Observable<PacketEnvelope> receive() {
            return channel.subscribeOn(Schedulers.io());
        }

        @Override
        public CompletableFuture<Void> shutdown() {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> inbound(PacketEnvelope packetEnvelope) {
            channel.onNext(packetEnvelope);
            return CompletableFuture.completedFuture(null);
        }
    }
}
