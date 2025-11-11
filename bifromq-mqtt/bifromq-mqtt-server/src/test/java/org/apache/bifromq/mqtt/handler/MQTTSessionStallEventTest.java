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

package org.apache.bifromq.mqtt.handler;

import static org.apache.bifromq.mqtt.handler.MQTTSessionIdUtil.userSessionId;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxResendTimes;
import static org.apache.bifromq.plugin.settingprovider.Setting.ResendTimeoutSeconds;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.util.concurrent.EventExecutor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.metrics.TenantMetric;
import org.apache.bifromq.mqtt.handler.v3.MQTT3PersistentSessionHandler;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.type.QoS;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MQTTSessionStallEventTest extends BaseSessionHandlerTest {

    private MQTT3PersistentSessionHandler handler;

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        // shrink stall timeout to 1s for fast tests
        when(settingProvider.provide(eq(MaxResendTimes), anyString())).thenReturn(1);
        when(settingProvider.provide(eq(ResendTimeoutSeconds), anyString())).thenReturn(1);

        ChannelDuplexHandler sessionHandlerAdder = buildChannelHandler();
        mockInboxReader();
        channel = new EmbeddedChannel(true, true, new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ch.attr(ChannelAttrs.MQTT_SESSION_CTX).set(sessionContext);
                ch.attr(ChannelAttrs.PEER_ADDR).set(new InetSocketAddress(remoteIp, remotePort));
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new ChannelTrafficShapingHandler(512 * 1024, 512 * 1024));
                pipeline.addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024));
                pipeline.addLast(sessionHandlerAdder);
            }
        });
        handler = (MQTT3PersistentSessionHandler) channel.pipeline().last();
    }

    @SneakyThrows
    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        channel.close();
        super.tearDown(method);
    }

    @Override
    protected ChannelDuplexHandler buildChannelHandler() {
        return new ChannelDuplexHandler() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                ctx.pipeline().addLast(MQTT3PersistentSessionHandler.builder()
                    .settings(new TenantSettings(tenantId, settingProvider))
                    .tenantMeter(tenantMeter)
                    .inboxVersion(InboxVersion.newBuilder().setMod(0).setIncarnation(0).build())
                    .oomCondition(oomCondition)
                    .userSessionId(userSessionId(clientInfo))
                    .keepAliveTimeSeconds(120)
                    .clientInfo(clientInfo)
                    .noDelayLWT(null)
                    .ctx(ctx)
                    .build());
                ctx.pipeline().remove(this);
            }
        };
    }

    @Test
    public void scheduleAndCancelOnWritable() {
        // prepare inflight by feeding one qos1 message
        mockCheckPermission(true);
        inboxFetchConsumer.accept(fetch(1, 128, QoS.AT_LEAST_ONCE));
        channel.runPendingTasks();

        reset(eventCollector);
        // simulate unwritable via a mocked ctx: schedule stall task
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        when(mockCtx.channel()).thenReturn(channel);
        EventExecutor executor = channel.eventLoop();
        when(mockCtx.executor()).thenReturn(executor);

        // first as false -> schedule stall check
        // we mock isWritable by delegating to a wrapper Channel returning false only for this call
        Channel fakeCh = Mockito.mock(Channel.class);
        when(fakeCh.isWritable()).thenReturn(false);
        when(fakeCh.eventLoop()).thenReturn(channel.eventLoop());
        when(fakeCh.pipeline()).thenReturn(channel.pipeline());
        when(fakeCh.bytesBeforeUnwritable()).thenReturn(0L);
        when(fakeCh.bytesBeforeWritable()).thenReturn(0L);
        when(fakeCh.config()).thenReturn(channel.config());
        when(mockCtx.channel()).thenReturn(fakeCh);
        handler.channelWritabilityChanged(mockCtx);

        // then as true -> cancel
        when(mockCtx.channel()).thenReturn(channel); // back to real channel which is writable
        handler.channelWritabilityChanged(mockCtx);

        // advance time beyond 1s, scheduled stall should have been canceled, so no event
        channel.advanceTimeBy(2, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();

        verify(eventCollector, times(0)).report(any(Event.class));
    }

    @Test
    public void fireOnceOnTimeoutWhenStillUnwritable() {
        mockCheckPermission(true);
        inboxFetchConsumer.accept(fetch(1, 128, QoS.AT_LEAST_ONCE));
        channel.runPendingTasks();

        // reset any previous events
        reset(eventCollector);
        // schedule using a fake ctx whose channel reports unwritable
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel fakeCh = Mockito.mock(Channel.class);
        when(fakeCh.isWritable()).thenReturn(false);
        when(fakeCh.eventLoop()).thenReturn(channel.eventLoop());
        when(fakeCh.pipeline()).thenReturn(channel.pipeline());
        when(fakeCh.bytesBeforeUnwritable()).thenReturn(0L);
        when(fakeCh.bytesBeforeWritable()).thenReturn(0L);
        when(fakeCh.config()).thenReturn(channel.config());
        when(mockCtx.channel()).thenReturn(fakeCh);
        when(mockCtx.executor()).thenReturn(channel.eventLoop());
        handler.channelWritabilityChanged(mockCtx);

        // keep unwritable at fire time by keeping fakeCh on schedule
        channel.advanceTimeBy(2, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();

        ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector, times(1)).report(captor.capture());
        assert captor.getValue().type() == EventType.SUB_STALLED;
        verify(tenantMeter, times(1)).recordCount(TenantMetric.MqttStalledCount);
    }

    @Test
    public void cancelOnChannelInactive() {
        mockCheckPermission(true);
        inboxFetchConsumer.accept(fetch(1, 128, QoS.AT_LEAST_ONCE));
        channel.runPendingTasks();

        reset(eventCollector);
        mockSessionReg();

        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel fakeCh = Mockito.mock(Channel.class);
        when(fakeCh.isWritable()).thenReturn(false);
        when(fakeCh.eventLoop()).thenReturn(channel.eventLoop());
        when(fakeCh.pipeline()).thenReturn(channel.pipeline());
        when(fakeCh.bytesBeforeUnwritable()).thenReturn(0L);
        when(fakeCh.bytesBeforeWritable()).thenReturn(0L);
        when(fakeCh.config()).thenReturn(channel.config());
        when(mockCtx.channel()).thenReturn(fakeCh);
        when(mockCtx.executor()).thenReturn(channel.eventLoop());
        handler.channelWritabilityChanged(mockCtx);

        channel.close();

        channel.advanceTimeBy(2, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();

        verify(eventCollector, times(0)).report(Mockito.argThat(e -> e.type() == EventType.SUB_STALLED));
        verify(tenantMeter, times(0)).recordCount(TenantMetric.MqttStalledCount);
    }
}
