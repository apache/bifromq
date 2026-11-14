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

package org.apache.bifromq.baserpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.grpc.CallOptions;
import io.grpc.EquivalentAddressGroup;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.inprocess.InProcessSocketAddress;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.InProcAware;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Exercises the transport wrapper used by ClientChannel with real RPC servers.
 */
public class InProcAwareTest {
    private static final MethodDescriptor.Marshaller<String> MARSHALLER = new MethodDescriptor.Marshaller<>() {
        @Override
        public InputStream stream(String value) {
            return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String parse(InputStream stream) {
            try {
                return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };
    private static final MethodDescriptor<String, String> UNARY = method("Unary", MethodDescriptor.MethodType.UNARY);
    private static final MethodDescriptor<String, String> STREAM =
        method("Stream", MethodDescriptor.MethodType.BIDI_STREAMING);

    @DataProvider
    public Object[][] transports() {
        return new Object[][] {
            {true, null, true},
            {false, null, true},
            {false, SslProvider.JDK, true},
            {false, SslProvider.OPENSSL, true},
            {false, SslProvider.JDK, false},
            {false, SslProvider.OPENSSL, false}
        };
    }

    @Test(dataProvider = "transports")
    public void exchangesUnaryAndStreamingMessages(boolean inProcess, SslProvider tls, boolean trustedClient)
        throws Exception {
        SelfSignedCertificate serverCert = new SelfSignedCertificate("localhost");
        SelfSignedCertificate clientCert = trustedClient ? serverCert : new SelfSignedCertificate("untrusted");
        SslContext serverContext = null;
        SslContext clientContext = null;
        Server server = null;
        ManagedChannel channel = null;
        NameResolverProvider resolver = null;
        try {
            if (tls != null) {
                serverContext = GrpcSslContexts.configure(
                        SslContextBuilder.forServer(serverCert.certificate(), serverCert.privateKey()), tls)
                    .trustManager(serverCert.certificate()).clientAuth(ClientAuth.REQUIRE).build();
                clientContext = GrpcSslContexts.configure(SslContextBuilder.forClient(), tls)
                    .trustManager(serverCert.certificate())
                    .keyManager(clientCert.certificate(), clientCert.privateKey()).build();
            }
            SocketAddress address;
            if (inProcess) {
                String name = InProcessServerBuilder.generateName();
                server = InProcessServerBuilder.forName(name).addService(echoService()).build().start();
                address = new InProcessSocketAddress(name);
            } else {
                NettyServerBuilder builder = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                    .addService(echoService());
                if (serverContext != null) {
                    builder.sslContext(serverContext);
                }
                server = builder.build().start();
                address = new InetSocketAddress("127.0.0.1", server.getPort());
            }
            resolver = resolver(address);
            NameResolverRegistry.getDefaultRegistry().register(resolver);
            String target = "compat:///echo";
            NettyChannelBuilder builder = NettyChannelBuilder.forTarget(target);
            if (clientContext == null) {
                builder.usePlaintext();
            } else {
                builder.sslContext(clientContext);
            }
            channel = InProcAware.wrap(target, builder).overrideAuthority("localhost").build();
            ManagedChannel connectedChannel = channel;
            if (!trustedClient) {
                StatusRuntimeException error = expectThrows(StatusRuntimeException.class,
                    () -> unary(connectedChannel, "rejected"));
                assertEquals(error.getStatus().getCode(), Status.Code.UNAVAILABLE);
                return;
            }
            String payload = "MQTT-\u03bb-".repeat(32768);
            assertEquals(unary(channel, payload), payload);

            CompletableFuture<List<String>> responses = new CompletableFuture<>();
            List<String> received = new ArrayList<>();
            StreamObserver<String> requests = ClientCalls.asyncBidiStreamingCall(
                channel.newCall(STREAM, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS)),
                new StreamObserver<>() {
                    @Override
                    public void onNext(String value) {
                        received.add(value);
                    }

                    @Override
                    public void onError(Throwable error) {
                        responses.completeExceptionally(error);
                    }

                    @Override
                    public void onCompleted() {
                        responses.complete(List.copyOf(received));
                    }
                });
            requests.onNext("");
            requests.onNext(payload);
            requests.onCompleted();
            assertEquals(responses.get(5, TimeUnit.SECONDS), List.of("", payload));
        } finally {
            if (channel != null) {
                channel.shutdownNow();
                assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            }
            if (server != null) {
                server.shutdownNow();
                assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
            }
            if (resolver != null) {
                NameResolverRegistry.getDefaultRegistry().deregister(resolver);
            }
            ReferenceCountUtil.release(clientContext);
            ReferenceCountUtil.release(serverContext);
            if (clientCert != serverCert) {
                clientCert.delete();
            }
            serverCert.delete();
        }
    }

    private static String unary(ManagedChannel channel, String payload) {
        return ClientCalls.blockingUnaryCall(channel, UNARY,
            CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), payload);
    }

    private static MethodDescriptor<String, String> method(String name, MethodDescriptor.MethodType type) {
        return MethodDescriptor.<String, String>newBuilder().setType(type)
            .setFullMethodName(MethodDescriptor.generateFullMethodName("compat.Echo", name))
            .setRequestMarshaller(MARSHALLER).setResponseMarshaller(MARSHALLER).build();
    }

    private static ServerServiceDefinition echoService() {
        return ServerServiceDefinition.builder("compat.Echo")
            .addMethod(UNARY, ServerCalls.asyncUnaryCall((request, response) -> {
                response.onNext(request);
                response.onCompleted();
            }))
            .addMethod(STREAM, ServerCalls.asyncBidiStreamingCall(response -> new StreamObserver<String>() {
                @Override
                public void onNext(String value) {
                    response.onNext(value);
                }

                @Override
                public void onError(Throwable error) {
                    response.onError(error);
                }

                @Override
                public void onCompleted() {
                    response.onCompleted();
                }
            })).build();
    }

    private static NameResolverProvider resolver(SocketAddress address) {
        return new NameResolverProvider() {
            @Override
            public NameResolver newNameResolver(URI uri, NameResolver.Args args) {
                if (!getDefaultScheme().equals(uri.getScheme())) {
                    return null;
                }
                return new NameResolver() {
                    @Override
                    public String getServiceAuthority() {
                        return "localhost";
                    }

                    @Override
                    public void start(Listener2 listener) {
                        listener.onResult(ResolutionResult.newBuilder()
                            .setAddresses(List.of(new EquivalentAddressGroup(address))).build());
                    }

                    @Override
                    public void shutdown() {
                    }
                };
            }

            @Override
            protected boolean isAvailable() {
                return true;
            }

            @Override
            protected int priority() {
                return 5;
            }

            @Override
            public String getDefaultScheme() {
                return "compat";
            }

            @Override
            public Collection<Class<? extends SocketAddress>> getProducedSocketAddressTypes() {
                return List.of(SocketAddress.class);
            }
        };
    }
}
