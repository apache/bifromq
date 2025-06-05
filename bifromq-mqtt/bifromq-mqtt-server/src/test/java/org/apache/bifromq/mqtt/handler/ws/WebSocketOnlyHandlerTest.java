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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WebSocketOnlyHandlerTest {
    private EmbeddedChannel channel;
    private final String websocketPath = "/mqtt";

    @BeforeMethod
    public void setUp() {
        channel = new EmbeddedChannel(new WebSocketOnlyHandler(websocketPath));
    }

    @Test
    public void testValidWebSocketUpgradeRequest() {
        // Creating a valid WebSocket upgrade request
        FullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, websocketPath);
        request.headers().set(HttpHeaderNames.HOST, "localhost");
        request.headers().set(HttpHeaderNames.CONNECTION, "Upgrade");
        request.headers().set(HttpHeaderNames.UPGRADE, "websocket");
        request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==");
        request.headers().set(HttpHeaderNames.SEC_WEBSOCKET_VERSION, "13");

        assertTrue(channel.writeInbound(request));
        assertNull(channel.readOutbound());
    }

    @Test
    public void testInvalidRequestPathMismatch() {
        // Creating an invalid WebSocket request with incorrect path
        FullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/wrongpath");
        request.headers().set(HttpHeaderNames.UPGRADE, "websocket");

        assertFalse(channel.writeInbound(request));
        FullHttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
    }

    @Test
    public void testInvalidRequestNoUpgradeHeader() {
        // Creating an HTTP request without WebSocket upgrade headers
        FullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, websocketPath);

        assertFalse(channel.writeInbound(request));
        FullHttpResponse response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
    }

    @Test
    public void testRequestCleanup() {
        // Ensure channel cleanup is handled correctly
        channel.finish();
        assertNull(channel.readInbound());
        assertNull(channel.readOutbound());
    }
}
