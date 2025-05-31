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

package org.apache.bifromq.mqtt.handler.ws;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WebSocketFrameToByteBufDecoderTest {
    private EmbeddedChannel channel;

    @BeforeMethod
    public void setUp() {
        // Initialize channel with the decoder before each test
        channel = new EmbeddedChannel(new WebSocketFrameToByteBufDecoder());
    }

    @Test
    public void testDecode() {
        // Creating a BinaryWebSocketFrame with sample data
        ByteBuf originalContent = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4, 5});
        BinaryWebSocketFrame frame = new BinaryWebSocketFrame(originalContent);

        // Write the frame to the channel
        assertTrue(channel.writeInbound(frame));

        // Read the decoded output from the channel
        ByteBuf decoded = channel.readInbound();

        assertNotNull(decoded);
        assertEquals(originalContent.readerIndex(), decoded.readerIndex());
        assertEquals(originalContent.writerIndex(), decoded.writerIndex());
        assertTrue(ByteBufUtil.equals(originalContent, decoded));

        // Cleanup
        decoded.release();

        assertFalse(channel.finish());
    }
}
