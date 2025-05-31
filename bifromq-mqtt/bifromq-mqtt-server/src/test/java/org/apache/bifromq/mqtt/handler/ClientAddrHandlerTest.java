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

import static org.apache.bifromq.mqtt.handler.ChannelAttrs.PEER_ADDR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.net.InetSocketAddress;
import org.testng.annotations.Test;

public class ClientAddrHandlerTest {

    @Test
    void testParseValidHeaders() {
        DefaultHttpRequest request = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/test"
        );
        request.headers().set("X-Real-IP", "1.2.3.4");
        request.headers().set("X-Real-Port", "5678");

        EmbeddedChannel channel = new EmbeddedChannel(new ClientAddrHandler());

        assertTrue(channel.writeInbound(request));

        assertNull(channel.pipeline().get(ClientAddrHandler.class));

        InetSocketAddress addr = channel.attr(PEER_ADDR).get();
        assertNotNull(addr);
        assertEquals(addr.getAddress().getHostAddress(), "1.2.3.4");
        assertEquals(addr.getPort(), 5678);

        Object read = channel.readInbound();
        assertSame(request, read);
    }

    @Test
    void testSkipWhenAttrAlreadySet() {
        DefaultHttpRequest request = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/skip"
        );

        EmbeddedChannel channel = new EmbeddedChannel();
        InetSocketAddress preset = new InetSocketAddress("9.8.7.6", 4321);
        channel.attr(PEER_ADDR).set(preset);
        channel.pipeline().addLast(new ClientAddrHandler());

        assertTrue(channel.writeInbound(request));

        assertNull(channel.pipeline().get(ClientAddrHandler.class));

        InetSocketAddress addr = channel.attr(PEER_ADDR).get();
        assertSame(preset, addr);

        Object read = channel.readInbound();
        assertSame(request, read);
    }

    @Test
    void testIgnoreMissingOrInvalidHeaders() {
        DefaultHttpRequest noHeaderReq = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/noheader"
        );
        EmbeddedChannel channel1 = new EmbeddedChannel(new ClientAddrHandler());
        assertTrue(channel1.writeInbound(noHeaderReq));
        assertNull(channel1.pipeline().get(ClientAddrHandler.class));
        assertNull(channel1.attr(PEER_ADDR).get());
        channel1.readInbound();

        DefaultHttpRequest invalidPortReq = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/invalidport"
        );
        invalidPortReq.headers().set("X-Real-IP", "5.6.7.8");
        invalidPortReq.headers().set("X-Real-Port", "notanint");
        EmbeddedChannel channel2 = new EmbeddedChannel(new ClientAddrHandler());
        assertTrue(channel2.writeInbound(invalidPortReq));
        assertNull(channel2.pipeline().get(ClientAddrHandler.class));
        InetSocketAddress addr2 = channel2.attr(PEER_ADDR).get();
        assertNotNull(addr2);
        assertEquals(addr2.getAddress().getHostAddress(), "5.6.7.8");
        assertEquals(addr2.getPort(), 0);
        channel2.readInbound();
    }
}
