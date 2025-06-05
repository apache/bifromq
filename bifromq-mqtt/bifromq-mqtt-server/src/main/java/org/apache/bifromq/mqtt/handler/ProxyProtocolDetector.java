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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Detects Proxy Protocol v1 or v2 headers on the first packet.
 * If header is found, lets HAProxyMessageDecoder handle it.
 * Otherwise, removes itself HAProxyMessageDecoder and ProxyProtocolHandler and forwards bytes downstream.
 */
public class ProxyProtocolDetector extends ByteToMessageDecoder {
    private static final byte[] PPV1_SIG = "PROXY ".getBytes(StandardCharsets.US_ASCII);

    private static final byte[] PPV2_SIG = {
        0x0D, 0x0A, 0x0D, 0x0A,
        0x00, 0x0D, 0x0A, 0x51,
        0x55, 0x49, 0x54, 0x0A
    };

    private static final int LEN = Math.max(PPV2_SIG.length + 4, PPV1_SIG.length);

    private boolean finished = false;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx, msg);
        if (finished) {
            ctx.pipeline().remove(this);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        int readable = in.readableBytes();
        int readerIndex = in.readerIndex();
        // Wait until at least enough bytes to detect either version
        if (readable < LEN) {
            return;
        }
        // Check for v2 signature
        boolean isV2 = true;
        for (int i = 0; i < PPV2_SIG.length; i++) {
            if (in.getByte(readerIndex + i) != PPV2_SIG[i]) {
                isV2 = false;
                break;
            }
        }
        // Check version nibble for v2
        if (isV2) {
            byte verCmd = in.getByte(readerIndex + 12);
            if ((verCmd & 0xF0) == 0x20) {
                // Detected PPv2
                // Forward all bytes as-is
                ByteBuf buf = in.readRetainedSlice(readable);
                out.add(buf);
                finished = true;
                return;
            }
        } else {
            // Check for v1 signature
            boolean isV1 = true;
            for (int i = 0; i < PPV1_SIG.length; i++) {
                if (in.getByte(readerIndex + i) != PPV1_SIG[i]) {
                    isV1 = false;
                    break;
                }
            }
            if (isV1) {
                // Detected PPv1
                // Forward all bytes as-is
                ByteBuf buf = in.readRetainedSlice(readable);
                out.add(buf);
                finished = true;
                return;
            }
        }
        // Not Proxy Protocol v1 or v2: remove both detector and decoder
        ctx.pipeline().remove(HAProxyMessageDecoder.class);
        ctx.pipeline().remove(ProxyProtocolHandler.class);
        // Forward all bytes as-is
        ByteBuf buf = in.readRetainedSlice(readable);
        out.add(buf);
        finished = true;
    }
}