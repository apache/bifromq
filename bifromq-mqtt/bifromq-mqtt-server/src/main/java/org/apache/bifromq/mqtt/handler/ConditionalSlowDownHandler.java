/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.mqtt.handler;

import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.mqtt.handler.condition.InboundResourceCondition;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import org.apache.bifromq.sysprops.props.MaxSlowDownTimeoutSeconds;
import org.apache.bifromq.type.ClientInfo;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConditionalSlowDownHandler extends ChannelInboundHandlerAdapter {
    public static final String NAME = "SlowDownHandler";
    private static final long MAX_SLOWDOWN_TIME =
        Duration.ofSeconds(MaxSlowDownTimeoutSeconds.INSTANCE.get()).toNanos();
    private final Condition slowDownCondition;
    private final Supplier<Long> nanoProvider;
    private final IEventCollector eventCollector;
    private final ClientInfo clientInfo;
    private ChannelHandlerContext ctx;
    private ScheduledFuture<?> resumeTask;
    private long slowDownAt = Long.MAX_VALUE;

    public ConditionalSlowDownHandler(Condition slowDownCondition,
                                      IEventCollector eventCollector,
                                      Supplier<Long> nanoProvider,
                                      ClientInfo clientInfo) {
        this.slowDownCondition = slowDownCondition;
        this.eventCollector = eventCollector;
        this.nanoProvider = nanoProvider;
        this.clientInfo = clientInfo;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        if (resumeTask != null) {
            resumeTask.cancel(true);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (slowDownCondition.meet()) {
            ctx.channel().config().setAutoRead(false);
            slowDownAt = nanoProvider.get();
            scheduleResumeRead();
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (!slowDownCondition.meet()) {
            ctx.channel().config().setAutoRead(true);
            slowDownAt = Long.MAX_VALUE;
        } else {
            closeIfNeeded();
        }
        ctx.fireChannelReadComplete();
    }

    private void scheduleResumeRead() {
        if (resumeTask == null || resumeTask.isDone()) {
            resumeTask = ctx.executor()
                .schedule(this::resumeRead, ThreadLocalRandom.current().nextLong(100, 1001), TimeUnit.MILLISECONDS);
        }
    }

    private void resumeRead() {
        if (!slowDownCondition.meet()) {
            if (!ctx.channel().config().isAutoRead()) {
                ctx.channel().config().setAutoRead(true);
                ctx.read();
                slowDownAt = Long.MAX_VALUE;
            }
        } else {
            if (!closeIfNeeded()) {
                resumeTask = null;
                scheduleResumeRead();
            }
        }
    }

    private boolean closeIfNeeded() {
        if (nanoProvider.get() - slowDownAt > MAX_SLOWDOWN_TIME) {
            ctx.close();
            if (slowDownCondition.meet()) {
                if (slowDownCondition instanceof InboundResourceCondition) {
                    eventCollector.report(getLocal(OutOfTenantResource.class)
                        .reason(slowDownCondition.toString())
                        .clientInfo(clientInfo));
                }
                eventCollector.report(getLocal(ResourceThrottled.class)
                    .reason(slowDownCondition.toString())
                    .clientInfo(clientInfo));
            }
            return true;
        }
        return false;
    }
}
