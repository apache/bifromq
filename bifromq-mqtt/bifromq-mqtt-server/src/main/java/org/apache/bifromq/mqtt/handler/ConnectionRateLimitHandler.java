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

import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ChannelError;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class ConnectionRateLimitHandler extends ChannelDuplexHandler {
    /**
     * Initialize the pipeline when the connection is accepted.
     */
    public interface ChannelPipelineInitializer {
        void initialize(ChannelPipeline pipeline);
    }

    private final RateLimiter rateLimiter;
    private final IEventCollector eventCollector;
    private final ChannelPipelineInitializer initializer;

    public ConnectionRateLimitHandler(RateLimiter limiter,
                                      IEventCollector eventCollector,
                                      ChannelPipelineInitializer initializer) {
        rateLimiter = limiter;
        this.eventCollector = eventCollector;
        this.initializer = initializer;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (rateLimiter.tryAcquire()) {
            initializer.initialize(ctx.pipeline());
            ctx.fireChannelActive();
        } else {
            log.debug("Connection dropped due to exceed limit");
            eventCollector.report(getLocal(ChannelError.class)
                .peerAddress(ChannelAttrs.socketAddress(ctx.channel()))
                .cause(new RuntimeException("Reject connection due to conn rate limiting")));
            // close the connection randomly
            ctx.channel().config().setAutoRead(false);
            ctx.executor().schedule(() -> {
                if (ctx.channel().isActive()) {
                    ctx.close();
                }
            }, ThreadLocalRandom.current().nextLong(100, 3000), TimeUnit.MILLISECONDS);
        }
    }
}
