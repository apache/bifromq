/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

public class ProxyProtocolDetectorTest {

    @Test
    public void testDetectPPv2() {
        // Construct minimal Proxy Protocol v2 header
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(new byte[] {
            0x0D, 0x0A, 0x0D, 0x0A,
            0x00, 0x0D, 0x0A, 0x51,
            0x55, 0x49, 0x54, 0x0A
        });
        buf.writeByte(0x21); // Version 2, PROXY
        buf.writeByte(0x11); // TCP over IPv4
        buf.writeShort(12);  // Address length
        buf.writeBytes(new byte[] {127, 0, 0, 1}); // Source IP
        buf.writeBytes(new byte[] {127, 0, 0, 1}); // Destination IP
        buf.writeShort(1234); // Source port
        buf.writeShort(1883); // Destination port

        EmbeddedChannel channel = new EmbeddedChannel(
            new ProxyProtocolDetector(),
            new HAProxyMessageDecoder()
        );

        assertTrue(channel.writeInbound(buf));
        HAProxyMessage msg = channel.readInbound();
        assertNotNull(msg);
        assertEquals(msg.sourceAddress(), "127.0.0.1");
        assertEquals(msg.sourcePort(), 1234);
        assertEquals(msg.destinationAddress(), "127.0.0.1");
        assertEquals(msg.destinationPort(), 1883);
        msg.release();

        // Detector should be removed, decoder remains
        assertNull(channel.pipeline().get(ProxyProtocolDetector.class));
        assertNull(channel.pipeline().get(HAProxyMessageDecoder.class));
    }

    @Test
    public void testDetectPPv1() {
        EmbeddedChannel channel = new EmbeddedChannel(
            new ProxyProtocolDetector(),
            new HAProxyMessageDecoder()
        );
        String header = "PROXY TCP4 1.2.3.4 5.6.7.8 4321 8765\r\n";
        ByteBuf buf = Unpooled.copiedBuffer(header, StandardCharsets.US_ASCII);

        assertTrue(channel.writeInbound(buf));
        HAProxyMessage msg = channel.readInbound();
        assertNotNull(msg);
        assertEquals(msg.sourceAddress(), "1.2.3.4");
        assertEquals(msg.sourcePort(), 4321);
        assertEquals(msg.destinationAddress(), "5.6.7.8");
        assertEquals(msg.destinationPort(), 8765);
        msg.release();

        assertNull(channel.pipeline().get(ProxyProtocolDetector.class));
        assertNull(channel.pipeline().get(HAProxyMessageDecoder.class));
    }

    @Test
    public void testNonProxyProtocol() {
        EmbeddedChannel channel = new EmbeddedChannel(
            new ProxyProtocolDetector(),
            new HAProxyMessageDecoder(),
            new ProxyProtocolHandler()
        );
        // prepare at least 16 bytes of payload
        byte[] payload = "HELLO WORLD MQTT ABCDEFGH".getBytes(StandardCharsets.US_ASCII);
        ByteBuf buf = Unpooled.copiedBuffer(payload);

        assertTrue(channel.writeInbound(buf));
        ByteBuf read = channel.readInbound();
        assertNotNull(read);
        byte[] readBytes = new byte[read.readableBytes()];
        read.readBytes(readBytes);
        assertArrayEquals(payload, readBytes);
        read.release();

        // Both detector and decoder should be removed
        assertNull(channel.pipeline().get(ProxyProtocolDetector.class));
        assertNull(channel.pipeline().get(HAProxyMessageDecoder.class));
    }
}
