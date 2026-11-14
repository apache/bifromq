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
import static org.testng.Assert.assertNull;
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
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ChannelAttrsTest {
    @DataProvider
    public Object[][] packetLimits() {
        int[][] limits = {
            {127, 125}, {128, 126}, {129, 127}, {130, 127}, {131, 128},
            {16383, 16380}, {16384, 16381}, {16386, 16383}, {16387, 16383}, {16388, 16384},
            {2097151, 2097147}, {2097155, 2097151}, {2097156, 2097151}, {2097157, 2097152}
        };
        List<Object[]> cases = new ArrayList<>();
        for (MqttVersion version : List.of(MqttVersion.MQTT_3_1_1, MqttVersion.MQTT_5)) {
            for (int[] limit : limits) {
                cases.add(new Object[] {version, limit[0], limit[1], false});
                cases.add(new Object[] {version, limit[0], limit[1], true});
            }
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "packetLimits")
    public void decoderUsesFullPacketLimit(MqttVersion version, int packetLimit, int remainingLength,
                                          boolean fragmented) {
        for (int extra = 0; extra <= 1; extra++) {
            EmbeddedChannel encoder = new EmbeddedChannel(MqttEncoder.INSTANCE);
            EmbeddedChannel decoder = new EmbeddedChannel();
            int propertiesLength = version == MqttVersion.MQTT_5 ? 1 : 0;
            ByteBuf source = Unpooled.wrappedBuffer(new byte[remainingLength + extra - 3 - propertiesLength]);
            try {
                decoder.pipeline().addLast(MqttDecoder.class.getName(), new MqttDecoder(256 * 1024));
                decoder.pipeline().addLast(new ChannelInboundHandlerAdapter());
                encoder.writeOutbound(MqttMessageBuilders.connect().protocolVersion(version)
                    .clientId("packet-limit").build());
                ByteBuf connect = encoder.readOutbound();
                decoder.writeInbound(connect);
                ReferenceCountUtil.release(decoder.readInbound());
                ChannelAttrs.setMaxPayload(packetLimit, decoder.pipeline().lastContext());
                encoder.writeOutbound(MqttMessageBuilders.publish().topicName("a").qos(MqttQoS.AT_MOST_ONCE)
                    .payload(source).build());
                ByteBuf encoded = encoder.readOutbound();
                assertEquals(encoded.readableBytes() <= packetLimit, extra == 0);
                if (fragmented) {
                    decoder.writeInbound(encoded.readRetainedSlice(1));
                    assertNull(decoder.readInbound());
                }
                decoder.writeInbound(encoded);
                MqttMessage decoded = decoder.readInbound();
                try {
                    if (extra == 0) {
                        assertTrue(decoded.decoderResult().isSuccess());
                    } else {
                        assertTrue(decoded.decoderResult().cause() instanceof TooLongFrameException);
                    }
                } finally {
                    ReferenceCountUtil.release(decoded);
                }
            } finally {
                source.release();
                encoder.finishAndReleaseAll();
                decoder.finishAndReleaseAll();
            }
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

}
