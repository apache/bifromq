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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.ReferenceCountUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ChannelAttrsTest {
    @DataProvider
    public Object[][] packetLimits() {
        return new Object[][] {
            {127, 125}, {128, 126}, {129, 127}, {130, 127}, {131, 128},
            {16383, 16380}, {16384, 16381}, {16386, 16383}, {16387, 16383}, {16388, 16384},
            {2097151, 2097147}, {2097155, 2097151}, {2097156, 2097151}, {2097157, 2097152}
        };
    }

    @Test(dataProvider = "packetLimits")
    public void decoderUsesFullPacketLimit(int packetLimit, int remainingLength) {
        for (int extra = 0; extra <= 1; extra++) {
            EmbeddedChannel encoder = new EmbeddedChannel(MqttEncoder.INSTANCE);
            EmbeddedChannel decoder = new EmbeddedChannel();
            ByteBuf source = Unpooled.wrappedBuffer(new byte[remainingLength + extra - 3]);
            try {
                decoder.pipeline().addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024));
                decoder.pipeline().addLast(new ChannelInboundHandlerAdapter());
                ChannelAttrs.setMaxPayload(packetLimit, decoder.pipeline().lastContext());
                encoder.writeOutbound(MqttMessageBuilders.publish().topicName("a").qos(MqttQoS.AT_MOST_ONCE)
                    .payload(source).build());
                ByteBuf encoded = encoder.readOutbound();
                assertEquals(encoded.readableBytes() <= packetLimit, extra == 0);
                decoder.writeInbound(encoded);
                MqttMessage decoded = decoder.readInbound();
                if (extra == 0) {
                    assertTrue(decoded.decoderResult().isSuccess());
                } else {
                    assertTrue(decoded.decoderResult().cause() instanceof TooLongFrameException);
                }
                ReferenceCountUtil.release(decoded);
            } finally {
                source.release();
                encoder.finishAndReleaseAll();
                decoder.finishAndReleaseAll();
            }
        }
    }

    @Test
    public void remainingLengthFitsMaximumPacket() {
        int[] limits = {3, 127, 128, 129, 130, 131, 16383, 16384, 16386, 16387, 16388,
            2097151, 2097155, 2097156, 2097157, 268435456};
        for (int limit : limits) {
            int remainingLength = ChannelAttrs.maxRemainingLength(limit);
            assertTrue(encodedSize(remainingLength) <= limit);
            assertTrue(encodedSize(remainingLength + 1) > limit);
        }
    }

    @Test
    public void minimumPacketLimitCreatesValidDecoder() {
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            channel.pipeline().addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024));
            channel.pipeline().addLast(new ChannelInboundHandlerAdapter());
            ChannelAttrs.setMaxPayload(3, channel.pipeline().lastContext());
            channel.writeInbound(Unpooled.wrappedBuffer(new byte[] {(byte) 0xc0, 0}));
            MqttMessage ping = channel.readInbound();
            assertTrue(ping.decoderResult().isSuccess());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private int encodedSize(int remainingLength) {
        int bytes = remainingLength + 2;
        while ((remainingLength >>= 7) != 0) {
            bytes++;
        }
        return bytes;
    }

}
