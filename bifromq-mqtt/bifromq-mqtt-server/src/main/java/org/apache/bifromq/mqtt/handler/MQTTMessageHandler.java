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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MQTTMessageHandler extends ChannelDuplexHandler {
    private static final int DEFAULT_FLUSH_AFTER_FLUSHES = 128;
    private final int explicitFlushAfterFlushes;
    private final Runnable flushTask;
    private int flushPendingCount;
    private Future<?> nextScheduledFlush;

    protected ChannelHandlerContext ctx;

    protected MQTTMessageHandler() {
        this(DEFAULT_FLUSH_AFTER_FLUSHES);
    }

    protected MQTTMessageHandler(int explicitFlushAfterFlushes) {
        this.explicitFlushAfterFlushes = explicitFlushAfterFlushes;
        this.flushTask = () -> {
            if (flushPendingCount > 0) {
                flushPendingCount = 0;
                nextScheduledFlush = null;
                ctx.flush();
            }
        };
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        flushIfNeeded(ctx);
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // To ensure we not miss to flush anything, do it now.
        flushIfNeeded(ctx);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        flushIfNeeded(ctx);
        ctx.disconnect(promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        flushIfNeeded(ctx);
        ctx.close(promise);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (!ctx.channel().isWritable()) {
            flushIfNeeded(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    protected ChannelFuture writeAndFlush(Object msg) {
        ChannelFuture future = ctx.write(msg);
        flush(false);
        return future;
    }

    protected void flush(boolean immediately) {
        if (++flushPendingCount == explicitFlushAfterFlushes || immediately) {
            flushNow(ctx);
        } else {
            scheduleFlush(ctx);
        }
    }

    private void flushIfNeeded(ChannelHandlerContext ctx) {
        if (flushPendingCount > 0) {
            flushNow(ctx);
        }
    }

    private void flushNow(ChannelHandlerContext ctx) {
        cancelScheduledFlush();
        flushPendingCount = 0;
        ctx.flush();
    }

    private void scheduleFlush(final ChannelHandlerContext ctx) {
        if (nextScheduledFlush == null) {
            // Run as soon as possible, but still yield to give a chance for additional writes to enqueue.
            nextScheduledFlush = ctx.executor().submit(flushTask);
        }
    }

    private void cancelScheduledFlush() {
        if (nextScheduledFlush != null) {
            nextScheduledFlush.cancel(false);
            nextScheduledFlush = null;
        }
    }
}
