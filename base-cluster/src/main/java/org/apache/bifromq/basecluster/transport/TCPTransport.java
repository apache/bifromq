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

import org.apache.bifromq.basecluster.transport.proto.Packet;
import org.apache.bifromq.baseenv.NettyEnv;
import org.apache.bifromq.basehlc.HLC;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.reactivex.rxjava3.core.Completable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class TCPTransport extends AbstractTransport {
    private final Counter sendBytes;
    private final Counter recvBytes;
    private final DistributionSummary transportLatency;
    private final ClientBridger clientBridger = new ClientBridger();
    private final ServerBridger serverBridger = new ServerBridger();
    private final ConcurrentMap<InetSocketAddress, LoadingCache<Integer, ChannelFuture>> channelMaps =
        new ConcurrentHashMap<>();
    private final ThreadLocal<Integer> threadChannelKey = new ThreadLocal<>();
    private final EventLoopGroup elg;
    private final TCPTransportOptions opts;
    private final AtomicInteger nextChannelKey = new AtomicInteger(0);
    private final Bootstrap clientBootstrap;
    private final ChannelFuture tcpListeningChannel;

    @Builder
    TCPTransport(@NonNull String env, InetSocketAddress bindAddr, SslContext serverSslContext,
                 SslContext clientSslContext, TCPTransportOptions opts) {
        super(env);
        try {
            Preconditions.checkArgument(opts.connTimeoutInMS > 0, "connTimeoutInMS must be a positive number");
            Preconditions.checkArgument(opts.maxBufferSizeInBytes > 0,
                "maxBufferSizeInBytes must be a positive number");
            Preconditions.checkArgument(opts.maxChannelsPerHost > 0, "maxChannelsPerHost must be a positive number");
            this.opts = opts.toBuilder().build();
            elg = NettyEnv.createEventLoopGroup(4, "cluster-tcp-transport");
            clientBootstrap = setupTcpClient(clientSslContext);
            tcpListeningChannel = setupTcpServer(bindAddr, serverSslContext);
            InetSocketAddress localAddress = (InetSocketAddress) tcpListeningChannel.channel().localAddress();
            Tags tags = Tags.of("proto", "tcp")
                .and("local", localAddress.getAddress().getHostAddress() + ":" + localAddress.getPort());
            sendBytes = Counter.builder("basecluster.send.bytes")
                .tags(tags)
                .register(Metrics.globalRegistry);
            recvBytes = Counter.builder("basecluster.recv.bytes")
                .tags(tags)
                .register(Metrics.globalRegistry);
            transportLatency = DistributionSummary.builder("basecluster.transport.latency")
                .tags(tags)
                .register(Metrics.globalRegistry);
            log.debug("Creating tcp transport: bindAddr={}", localAddress);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize tcp transport", e);
        }
    }

    private static void trace(String format, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(format, args);
        }
    }

    private Bootstrap setupTcpClient(SslContext sslContext) {
        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(elg)
            .channel(NettyEnv.getSocketChannelClass())
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                new WriteBufferWaterMark(opts.maxBufferSizeInBytes / 2, opts.maxBufferSizeInBytes))
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, opts.connTimeoutInMS);
        clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) {
                if (sslContext != null) {
                    channel.pipeline().addLast("ssl", sslContext.newHandler(channel.alloc()));
                }
                channel.pipeline()
                    .addLast("probe", new ProbeHandler())
                    .addLast("frameDecoder", new ProtobufVarint32FrameDecoder())
                    .addLast("protoBufDecoder", new ProtobufDecoder(Packet.getDefaultInstance()))
                    .addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender())
                    .addLast("protoBufEncoder", new ProtobufEncoder())
                    .addLast("bridger", clientBridger);
            }
        });
        return clientBootstrap;
    }

    @SneakyThrows
    private ChannelFuture setupTcpServer(InetSocketAddress serverAddr, SslContext sslContext) {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        return serverBootstrap.group(elg)
            .channel(NettyEnv.getServerSocketChannelClass())
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .localAddress(serverAddr)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel channel) {
                    if (sslContext != null) {
                        channel.pipeline().addLast("ssl", sslContext.newHandler(channel.alloc()));
                    }
                    channel.pipeline()
                        .addLast("probe", new ProbeHandler())
                        .addLast("frameDecoder", new ProtobufVarint32FrameDecoder())
                        .addLast("protoBufDecoder", new ProtobufDecoder(Packet.getDefaultInstance()))
                        .addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender())
                        .addLast("protoBufEncoder", new ProtobufEncoder())
                        .addLast("Bridger", serverBridger);
                }
            })
            .bind()
            .sync();
    }

    @Override
    public InetSocketAddress bindAddress() {
        return (InetSocketAddress) tcpListeningChannel.channel().localAddress();
    }

    @Override
    protected CompletableFuture<Void> doSend(Packet packet, InetSocketAddress recipient) {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        getChannel(recipient).whenComplete((ch, e) -> {
            if (e != null) {
                log.debug("Failed to connect to address: {}, cause={}", recipient, e.getMessage());
                onDone.completeExceptionally(e);
            } else {
                if (ch.isWritable()) {
                    long packetLength = packet.getSerializedSize();
                    sendBytes.increment(packetLength);
                    ch.writeAndFlush(packet);
                    onDone.complete(null);
                } else {
                    onDone.completeExceptionally(new RuntimeException("Channel is not writable"));
                }
            }
        });
        return onDone;
    }

    @Override
    protected Completable doShutdown() {
        log.debug("Closing tcp transport");
        return Completable.concatArrayDelayError(
                Completable.fromRunnable(() -> channelMaps.forEach((r, cm) -> cm.invalidateAll())),
                Completable.fromFuture(tcpListeningChannel.channel().close()),
                Completable.fromFuture(elg.shutdownGracefully(0, 5, TimeUnit.SECONDS)),
                Completable.fromRunnable(() -> {
                    Metrics.globalRegistry.remove(sendBytes);
                    Metrics.globalRegistry.remove(recvBytes);
                    Metrics.globalRegistry.remove(transportLatency);
                }))
            .onErrorComplete();
    }

    @VisibleForTesting
    CompletableFuture<Channel> getChannel(InetSocketAddress recipient) {
        if (threadChannelKey.get() == null) {
            threadChannelKey.set(nextChannelKey.getAndUpdate(k -> (k + 1) % opts.maxChannelsPerHost));
        }
        Integer channelKey = threadChannelKey.get();
        ChannelFuture cf = channelMaps.compute(recipient, (r, cm) -> {
            if (cm == null) {
                cm = Caffeine.newBuilder()
                    .scheduler(Scheduler.systemScheduler())
                    .expireAfterAccess(Duration.ofSeconds(opts.idleTimeoutInSec))
                    .removalListener((Integer k, ChannelFuture v, RemovalCause cause) -> {
                        if (v != null && v.isDone() && v.channel().isActive()) {
                            trace("Closing #{} channel: remote={}, cause={}", k, v.channel().remoteAddress(),
                                cause);
                            v.channel().close();
                        }
                    })
                    .build((Integer k) -> {
                        trace("Setup #{} channel: remote={}", k, recipient);
                        return clientBootstrap.connect(recipient);
                    });
            }
            return cm;
        }).get(channelKey);
        if (cf.isDone()) {
            if (cf.channel().isActive()) {
                return CompletableFuture.completedFuture(cf.channel());
            } else {
                // channel is inactive rebuild one
                synchronized (cf) {
                    if (cf == channelMaps.get(recipient).get(channelKey)) {
                        log.debug("Rebuild #{} channel: remote={}", channelKey, recipient);
                        channelMaps.get(recipient).invalidate(channelKey);
                    }
                }
                return getChannel(recipient);
            }
        } else {
            CompletableFuture<Channel> f = new CompletableFuture<>();
            cf.addListener((ChannelFuture future) -> {
                if (future.isSuccess() && future.channel().isActive()) {
                    f.complete(future.channel());
                } else {
                    try {
                        future.get();
                    } catch (Exception e) {
                        f.completeExceptionally(e);
                    }
                }
            });
            return f;
        }
    }

    @Builder(toBuilder = true)
    @Accessors(chain = true, fluent = true)
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class TCPTransportOptions {
        @Builder.Default
        private int connTimeoutInMS = 5000;
        @Builder.Default
        private int idleTimeoutInSec = 5;
        @Builder.Default
        private int maxChannelsPerHost = 5;
        @Builder.Default
        private int maxBufferSizeInBytes = WriteBufferWaterMark.DEFAULT.high();
    }

    @ChannelHandler.Sharable
    private static class ClientBridger extends ChannelInboundHandlerAdapter {

        public ClientBridger() {
            super();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            trace("Outbound channel active: remote={}", ctx.channel().remoteAddress());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            trace("Outbound channel inactive: remote={}", ctx.channel().remoteAddress());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("Outbound channel failure: remote={}, cause={}", ctx.channel().remoteAddress(),
                cause.getMessage());
            ctx.close();
        }
    }

    @ChannelHandler.Sharable
    private class ServerBridger extends SimpleChannelInboundHandler<Packet> {

        public ServerBridger() {
            super();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
            trace("Received message: remote={}", ctx.channel().remoteAddress());
            recvBytes.increment(packet.getSerializedSize());
            transportLatency.record(HLC.INST.getPhysical(packet.getHlc() - HLC.INST.get()));
            doReceive(packet, (InetSocketAddress) ctx.channel().remoteAddress(),
                (InetSocketAddress) ctx.channel().localAddress());
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            trace("Inbound channel active: remote={}", ctx.channel().remoteAddress());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            trace("Inbound channel inactive: remote={}", ctx.channel().remoteAddress());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("Inbound channel failure: remote={}, cause={}", ctx.channel().remoteAddress(), cause.getMessage());
            ctx.close();
        }
    }
}
