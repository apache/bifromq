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

import static io.netty.handler.codec.ByteToMessageDecoder.MERGE_CUMULATOR;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.FastLzFrameDecoder;

public class ProbeHandler extends ChannelInboundHandlerAdapter {
    static final int MAGIC_NUMBER_FAST_LZ = 'F' << 16 | 'L' << 8 | 'Z';
    static final int MAGIC_NUMBER_BYTES = 4;

    private ByteBuf cumulation;
    private final ByteToMessageDecoder.Cumulator cumulator = MERGE_CUMULATOR;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf in = (ByteBuf) msg;
            boolean first = cumulation == null;
            cumulation = cumulator.cumulate(ctx.alloc(),
                first ? Unpooled.EMPTY_BUFFER : cumulation, (ByteBuf) msg);
            if (cumulation.readableBytes() < MAGIC_NUMBER_BYTES) {
                return;
            }

            int magic = in.getUnsignedMedium(0);
            if (magic == MAGIC_NUMBER_FAST_LZ) {
                ctx.pipeline().addAfter("probe", "decompressor", new FastLzFrameDecoder());
            }
            ctx.pipeline().remove("probe");
            super.channelRead(ctx, cumulation);
        } else {
            super.channelRead(ctx, msg);
        }
    }
}
