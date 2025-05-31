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

package org.apache.bifromq.mqtt.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.ReferenceCountUtil;
import java.util.LinkedList;
import java.util.Queue;

public class MQTTMessageDebounceHandler extends ChannelDuplexHandler {
    public static final String NAME = "MQTTMessageDebounceHandler";

    private final Queue<MqttMessage> buffer = new LinkedList<>();
    private boolean readOne = false;

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        MqttMessage msg;
        while ((msg = buffer.poll()) != null) {
            ReferenceCountUtil.release(msg);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void read(ChannelHandlerContext ctx) {
        if (ctx.channel().config().isAutoRead()) {
            MqttMessage msg;
            while ((msg = buffer.poll()) != null) {
                ctx.fireChannelRead(msg);
            }
            ctx.read();
        } else {
            MqttMessage msg = buffer.poll();
            if (msg != null) {
                ctx.fireChannelRead(msg);
            } else {
                readOne = true;
                ctx.read();
            }
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof MqttMessage;
        if (ctx.channel().config().isAutoRead()) {
            ctx.fireChannelRead(msg);
        } else {
            buffer.offer((MqttMessage) msg);
            if (readOne) {
                MqttMessage mqttMsg = buffer.poll();
                ctx.fireChannelRead(mqttMsg);
                readOne = false;
            }
        }
    }
}
