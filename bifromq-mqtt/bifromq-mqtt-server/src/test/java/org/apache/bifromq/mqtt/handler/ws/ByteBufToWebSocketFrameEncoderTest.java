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

public class ByteBufToWebSocketFrameEncoderTest {
    private EmbeddedChannel channel;

    @BeforeMethod
    public void setUp() {
        // Initialize channel with the encoder before each test
        channel = new EmbeddedChannel(new ByteBufToWebSocketFrameEncoder());
    }

    @Test
    public void testEncode() {
        // Creating a test ByteBuf with sample data
        ByteBuf input = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4, 5});

        // Write the ByteBuf to the channel
        input.retain();
        assertTrue(channel.writeOutbound(input.duplicate()));

        // Read the encoded output from the channel
        BinaryWebSocketFrame frame = channel.readOutbound();

        assertNotNull(frame);
        assertEquals(input.readerIndex(), frame.content().readerIndex());
        assertEquals(input.writerIndex(), frame.content().writerIndex());
        assertTrue(ByteBufUtil.equals(input, frame.content()));

        // Cleanup
        frame.release();

        assertFalse(channel.finish());
    }
}
