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

import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ChannelError;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConditionalRejectHandler extends ChannelInboundHandlerAdapter {
    public static final String NAME = "ConditionalRejectHandler";
    private final Condition rejectCondition;
    private final IEventCollector eventCollector;

    public ConditionalRejectHandler(Condition rejectCondition, IEventCollector eventCollector) {
        this.rejectCondition = rejectCondition;
        this.eventCollector = eventCollector;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (rejectCondition.meet()) {
            log.debug("Reject connection due to {}: remote={}", rejectCondition, ctx.channel().remoteAddress());
            // ensure no read will happen
            ctx.channel().config().setAutoRead(false);
            ctx.close();
            eventCollector.report(getLocal(ChannelError.class)
                .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))
                .cause(new RuntimeException("Reject connection due to " + rejectCondition)));
            return;
        }
        super.channelActive(ctx);
    }
}
