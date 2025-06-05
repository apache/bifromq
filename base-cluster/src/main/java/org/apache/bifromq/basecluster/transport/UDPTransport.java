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
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultAddressedEnvelope;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.DatagramPacketEncoder;
import io.netty.handler.codec.compression.FastLzFrameDecoder;
import io.netty.handler.codec.compression.FastLzFrameEncoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.subjects.CompletableSubject;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class UDPTransport extends AbstractTransport {
    private final Counter sendBytes;
    private final Counter recvBytes;
    private final DistributionSummary transportLatency;
    private final EventLoopGroup elg;
    private final Channel channel;
    private final Bridger bridger;
    private final InetSocketAddress localAddress;

    @Builder
    UDPTransport(@NonNull String env, InetSocketAddress bindAddr) {
        super(env);
        try {
            bridger = new Bridger();
            elg = NettyEnv.createEventLoopGroup(4, "cluster-udp-transport");
            Bootstrap bootstrap = new Bootstrap();
            channel = bootstrap.group(elg)
                .channel(NettyEnv.getDatagramChannelClass())
                .localAddress(bindAddr)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel channel) {
                        channel.pipeline()
                            .addLast("compressor", new FastLzFrameDecoder())
                            .addLast("decompressor", new FastLzFrameEncoder())
                            .addLast("udpEncoder", new DatagramPacketEncoder<>(new ProtobufEncoder()))
                            .addLast("Bridger", bridger);
                    }
                })
                .bind()
                .sync()
                .channel();
            localAddress = (InetSocketAddress) channel.localAddress();
            Tags tags = Tags.of("proto", "udp")
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

            log.debug("Creating udp transport: bindAddr={}", localAddress);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize udp transport", e);
        }
    }

    @Override
    public InetSocketAddress bindAddress() {
        return localAddress;
    }

    @Override
    protected CompletableFuture<Void> doSend(Packet packet, InetSocketAddress recipient) {

        log.trace("Sending packet via udp: size={}, packet#={}, recipient={}",
            packet.getSerializedSize(), packet.hashCode(), recipient);
        sendBytes.increment(packet.getSerializedSize());
        CompletableFuture<Void> ret = new CompletableFuture<>();
        channel.writeAndFlush(new DefaultAddressedEnvelope<>(packet, recipient)).addListener(future -> {
            if (!future.isSuccess()) {
                log.warn("failed to send packet via udp, recipient={}", recipient, future.cause());
            }
            ret.complete(null);
        });
        return ret;
    }

    @Override
    protected Completable doShutdown() {
        log.debug("Closing udp transport");
        CompletableSubject doneSignal = CompletableSubject.create();
        Completable.concatArrayDelayError(
                Completable.fromFuture(channel.close()),
                Completable.fromFuture(elg.shutdownGracefully(0, 5, TimeUnit.SECONDS)),
                Completable.fromRunnable(() -> {
                    Metrics.globalRegistry.remove(sendBytes);
                    Metrics.globalRegistry.remove(recvBytes);
                    Metrics.globalRegistry.remove(transportLatency);
                }))
            .onErrorComplete()
            .subscribe(doneSignal::onComplete);
        return doneSignal;
    }

    @ChannelHandler.Sharable
    private class Bridger extends SimpleChannelInboundHandler<DatagramPacket> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket dp) {
            // ctx.channel().remoteAddress() is null when DatagramChannel is not in 'connected' mode
            recvBytes.increment(dp.content().readableBytes());
            try {
                byte[] data = new byte[dp.content().readableBytes()];
                dp.content().readBytes(data);
                Packet packet = Packet.parseFrom(data);
                transportLatency.record(HLC.INST.getPhysical(packet.getHlc() - HLC.INST.get()));
                doReceive(packet, dp.sender(), dp.recipient());
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to decode packet, just ignore");
            }
        }
    }
}
