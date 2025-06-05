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

package org.apache.bifromq.basecluster.transport;

import com.google.protobuf.ByteString;
import io.netty.handler.ssl.SslContext;
import io.reactivex.rxjava3.core.Observable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

public final class Transport implements ITransport {
    private final ITransport tcpTransport;
    private final ITransport udpTransport;
    private final TransportOptions options;
    private final Observable<PacketEnvelope> sink;

    @Builder
    Transport(InetSocketAddress bindAddr,
              SslContext serverSslContext,
              SslContext clientSslContext,
              String env,
              TransportOptions options) {
        env = env == null ? "" : env;
        this.options = options == null ? new TransportOptions() : options.toBuilder().build();
        if (bindAddr == null) {
            bindAddr = new InetSocketAddress(0);
        }
        tcpTransport = TCPTransport.builder()
            .bindAddr(bindAddr)
            .env(env)
            .serverSslContext(serverSslContext)
            .clientSslContext(clientSslContext)
            .opts(this.options.tcpTransportOptions)
            .build();
        // bind to same address
        udpTransport = UDPTransport.builder()
            .env(env)
            .bindAddr(tcpTransport.bindAddress())
            .build();

        sink = Observable.merge(tcpTransport.receive(), udpTransport.receive());
    }

    @Override
    public InetSocketAddress bindAddress() {
        return tcpTransport.bindAddress();
    }

    public CompletableFuture<Void> send(List<ByteString> data, InetSocketAddress recipient) {
        int size = data.stream().map(ByteString::size).reduce(0, Integer::sum);
        try {
            if (Boolean.TRUE.equals(RELIABLE.get()) || size > options.mtu) {
                return tcpTransport.send(data, recipient);
            } else {
                return udpTransport.send(data, recipient);
            }
        } finally {
            RELIABLE.set(false);
        }
    }

    public Observable<PacketEnvelope> receive() {
        return sink;
    }

    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.allOf(
            tcpTransport.shutdown().exceptionally(e -> null),
            udpTransport.shutdown().exceptionally(e -> null)
        );
    }

    @Builder(toBuilder = true)
    @Accessors(chain = true, fluent = true)
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TransportOptions {
        @Builder.Default
        private long mtu = 1400;
        @Builder.Default
        private TCPTransport.TCPTransportOptions tcpTransportOptions = new TCPTransport.TCPTransportOptions();
    }
}
