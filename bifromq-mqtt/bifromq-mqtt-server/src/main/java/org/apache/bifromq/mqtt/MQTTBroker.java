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

package org.apache.bifromq.mqtt;

import static org.apache.bifromq.mqtt.handler.condition.ORCondition.or;

import org.apache.bifromq.baseenv.NettyEnv;
import org.apache.bifromq.mqtt.handler.ChannelAttrs;
import org.apache.bifromq.mqtt.handler.ClientAddrHandler;
import org.apache.bifromq.mqtt.handler.ConditionalRejectHandler;
import org.apache.bifromq.mqtt.handler.ConnectionRateLimitHandler;
import org.apache.bifromq.mqtt.handler.MQTTMessageDebounceHandler;
import org.apache.bifromq.mqtt.handler.MQTTPreludeHandler;
import org.apache.bifromq.mqtt.handler.ProxyProtocolDetector;
import org.apache.bifromq.mqtt.handler.ProxyProtocolHandler;
import org.apache.bifromq.mqtt.handler.condition.DirectMemPressureCondition;
import org.apache.bifromq.mqtt.handler.condition.HeapMemPressureCondition;
import org.apache.bifromq.mqtt.handler.ws.MqttOverWSHandler;
import org.apache.bifromq.mqtt.handler.ws.WebSocketOnlyHandler;
import org.apache.bifromq.mqtt.service.ILocalSessionServer;
import org.apache.bifromq.mqtt.session.MQTTSessionContext;
import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class MQTTBroker implements IMQTTBroker {
    private static final String MQTT_SUBPROTOCOL_CSV_LIST = "mqtt, mqttv3.1, mqttv3.1.1";
    private final MQTTBrokerBuilder builder;
    private final ILocalSessionServer sessionServer;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final RateLimiter connRateLimiter;
    private MQTTSessionContext sessionContext;
    private ChannelFuture tcpChannelF;
    private ChannelFuture tlsChannelF;
    private ChannelFuture wsChannelF;
    private ChannelFuture wssChannelF;

    public MQTTBroker(MQTTBrokerBuilder builder) {
        this.builder = builder;
        bossGroup = NettyEnv.createEventLoopGroup(builder.mqttBossELGThreads, "mqtt-boss-elg");
        new NettyEventExecutorMetrics(bossGroup).bindTo(Metrics.globalRegistry);
        workerGroup = NettyEnv.createEventLoopGroup(builder.mqttWorkerELGThreads, "mqtt-worker-elg");
        new NettyEventExecutorMetrics(workerGroup).bindTo(Metrics.globalRegistry);
        connRateLimiter = RateLimiter.create(builder.connectRateLimit);
        new NettyEventExecutorMetrics(bossGroup).bindTo(Metrics.globalRegistry);
        new NettyEventExecutorMetrics(workerGroup).bindTo(Metrics.globalRegistry);
        sessionServer = ILocalSessionServer.builder()
            .rpcServerBuilder(builder.rpcServerBuilder)
            .sessionRegistry(builder.sessionRegistry)
            .distService(builder.distService)
            .build();
    }

    @Override
    public final void start() {
        try {
            sessionContext = MQTTSessionContext.builder()
                .serverId(builder.brokerId())
                .localSessionRegistry(builder.sessionRegistry)
                .localDistService(builder.distService)
                .authProvider(builder.authProvider)
                .resourceThrottler(builder.resourceThrottler)
                .eventCollector(builder.eventCollector)
                .settingProvider(builder.settingProvider)
                .distClient(builder.distClient)
                .inboxClient(builder.inboxClient)
                .retainClient(builder.retainClient)
                .sessionDictClient(builder.sessionDictClient)
                .clientBalancer(builder.clientBalancer)
                .build();
            log.info("Starting MQTT broker");
            log.debug("Starting server channel");
            if (builder.tcpListenerBuilder != null) {
                tcpChannelF = this.bindTCPChannel(builder.tcpListenerBuilder);
                Channel channel = tcpChannelF.sync().channel();
                log.debug("Accepting mqtt connection over tcp channel at {}", channel.localAddress());
            }
            if (builder.tlsListenerBuilder != null) {
                tlsChannelF = this.bindTLSChannel(builder.tlsListenerBuilder);
                Channel channel = tlsChannelF.sync().channel();
                log.debug("Accepting mqtt connection over tls channel at {}", channel.localAddress());
            }
            if (builder.wsListenerBuilder != null) {
                wsChannelF = this.bindWSChannel(builder.wsListenerBuilder);
                Channel channel = wsChannelF.sync().channel();
                log.debug("Accepting mqtt connection over ws channel at {}", channel.localAddress());
            }
            if (builder.wssListenerBuilder != null) {
                wssChannelF = this.bindWSSChannel(builder.wssListenerBuilder);
                Channel channel = wssChannelF.sync().channel();
                log.debug("Accepting mqtt connection over wss channel at {}", channel.localAddress());
            }
            log.info("MQTT broker started");
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public final void close() {
        log.info("Stopping MQTT broker");
        if (tcpChannelF != null) {
            tcpChannelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over tcp channel");
        }
        if (tlsChannelF != null) {
            tlsChannelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over tls channel");
        }
        if (wsChannelF != null) {
            wsChannelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over ws channel");
        }
        if (wssChannelF != null) {
            wssChannelF.channel().close().syncUninterruptibly();
            log.debug("Stopped accepting mqtt connection over wss channel");
        }
        sessionContext.localSessionRegistry.disconnectAll(builder.disconnectRate).join();
        log.debug("All mqtt connection closed");

        sessionContext.awaitBgTasksFinish().join();
        log.debug("All background tasks done");

        bossGroup.shutdownGracefully().syncUninterruptibly();
        log.debug("Boss group shutdown");
        workerGroup.shutdownGracefully().syncUninterruptibly();
        log.debug("Worker group shutdown");
        log.info("MQTT broker stopped");
    }

    private ChannelFuture bindTCPChannel(ConnListenerBuilder.TCPConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new MQTTChannelInitializer() {
            @Override
            protected void initChannel(SocketChannel ch) {
                super.initChannel(ch);
                ch.pipeline().addLast("connRateLimiter", new ConnectionRateLimitHandler(connRateLimiter,
                    builder.eventCollector, p -> {
                    p.addLast("trafficShaper",
                        new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                    p.addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
                    // insert PacketFilter here
                    p.addLast(MqttDecoder.class.getName(), new MqttDecoder(builder.maxBytesInMessage));
                    p.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                    p.addLast(ConditionalRejectHandler.NAME,
                        new ConditionalRejectHandler(
                            or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE),
                            sessionContext.eventCollector));
                    p.addLast(MQTTPreludeHandler.NAME,
                        new MQTTPreludeHandler(builder.connectTimeoutSeconds));
                }));
            }
        });
    }

    private ChannelFuture bindTLSChannel(ConnListenerBuilder.TLSConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new MQTTChannelInitializer() {
            @Override
            protected void initChannel(SocketChannel ch) {
                super.initChannel(ch);
                ch.pipeline().addLast("connRateLimiter", new ConnectionRateLimitHandler(connRateLimiter,
                    builder.eventCollector, p -> {
                    p.addLast("ssl", connBuilder.sslContext.newHandler(ch.alloc()));
                    p.addLast("trafficShaper",
                        new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                    p.addLast(MqttEncoder.class.getName(), MqttEncoder.INSTANCE);
                    // insert PacketFilter here
                    p.addLast(MqttDecoder.class.getName(), new MqttDecoder(builder.maxBytesInMessage));
                    p.addLast(MQTTMessageDebounceHandler.NAME, new MQTTMessageDebounceHandler());
                    p.addLast(ConditionalRejectHandler.NAME,
                        new ConditionalRejectHandler(
                            or(DirectMemPressureCondition.INSTANCE, HeapMemPressureCondition.INSTANCE),
                            sessionContext.eventCollector));
                    p.addLast(MQTTPreludeHandler.NAME,
                        new MQTTPreludeHandler(builder.connectTimeoutSeconds));
                }));
            }
        });
    }

    private ChannelFuture bindWSChannel(ConnListenerBuilder.WSConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new MQTTChannelInitializer() {
            @Override
            protected void initChannel(SocketChannel ch) {
                super.initChannel(ch);
                ch.pipeline().addLast("connRateLimiter", new ConnectionRateLimitHandler(connRateLimiter,
                    builder.eventCollector, p -> {
                    p.addLast("trafficShaper",
                        new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                    p.addLast("httpEncoder", new HttpResponseEncoder());
                    p.addLast("httpDecoder", new HttpRequestDecoder());
                    p.addLast("remoteAddr", new ClientAddrHandler());
                    p.addLast("aggregator", new HttpObjectAggregator(65536));
                    p.addLast("webSocketOnly", new WebSocketOnlyHandler(connBuilder.path()));
                    p.addLast("webSocketHandler", new WebSocketServerProtocolHandler(connBuilder.path(),
                        MQTT_SUBPROTOCOL_CSV_LIST));
                    p.addLast("webSocketHandshakeListener", new MqttOverWSHandler(
                        builder.maxBytesInMessage, builder.connectTimeoutSeconds, sessionContext.eventCollector));
                }));
            }
        });
    }

    private ChannelFuture bindWSSChannel(ConnListenerBuilder.WSSConnListenerBuilder connBuilder) {
        return buildChannel(connBuilder, new MQTTChannelInitializer() {
            @Override
            protected void initChannel(SocketChannel ch) {
                super.initChannel(ch);
                ch.pipeline().addLast("connRateLimiter", new ConnectionRateLimitHandler(connRateLimiter,
                    builder.eventCollector, p -> {
                    p.addLast("ssl", connBuilder.sslContext.newHandler(ch.alloc()));
                    p.addLast("trafficShaper",
                        new ChannelTrafficShapingHandler(builder.writeLimit, builder.readLimit));
                    p.addLast("httpEncoder", new HttpResponseEncoder());
                    p.addLast("httpDecoder", new HttpRequestDecoder());
                    p.addLast(ClientAddrHandler.class.getName(), new ClientAddrHandler());
                    p.addLast("aggregator", new HttpObjectAggregator(65536));
                    p.addLast("webSocketOnly", new WebSocketOnlyHandler(connBuilder.path()));
                    p.addLast("webSocketHandler", new WebSocketServerProtocolHandler(connBuilder.path(),
                        MQTT_SUBPROTOCOL_CSV_LIST));
                    p.addLast("webSocketHandshakeListener", new MqttOverWSHandler(
                        builder.maxBytesInMessage, builder.connectTimeoutSeconds, sessionContext.eventCollector));
                }));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <T extends ConnListenerBuilder<T>> ChannelFuture buildChannel(T builder,
                                                                          final MQTTChannelInitializer chInitializer) {
        ServerBootstrap b = new ServerBootstrap().group(bossGroup, workerGroup)
            .channel(NettyEnv.determineServerSocketChannelClass(bossGroup))
            .childHandler(chInitializer)
            .childAttr(ChannelAttrs.MQTT_SESSION_CTX, sessionContext);
        builder.options.forEach((k, v) -> b.option((ChannelOption<? super Object>) k, v));
        builder.childOptions.forEach((k, v) -> b.childOption((ChannelOption<? super Object>) k, v));
        // Bind and start to accept incoming connections.
        return b.bind(builder.host, builder.port);
    }

    private abstract static class MQTTChannelInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            // handler for proxy protocol v1 and v2
            pipeline
                .addLast(ProxyProtocolDetector.class.getName(), new ProxyProtocolDetector())
                .addLast(HAProxyMessageDecoder.class.getName(), new HAProxyMessageDecoder())
                .addLast(ProxyProtocolHandler.class.getName(), new ProxyProtocolHandler());
        }
    }
}
