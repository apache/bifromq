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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import java.net.InetSocketAddress;
import org.testng.annotations.Test;

public class ProxyProtocolHandlerTest {

    @Test
    public void testHAProxyMessageParsedCorrectly() {
        // Prepare a HAProxyMessage
        String srcAddr = "192.168.1.100";
        String dstAddr = "10.0.0.1";
        int srcPort = 54321;
        int dstPort = 1883;

        HAProxyMessage message = new HAProxyMessage(
            HAProxyProtocolVersion.V2,
            HAProxyCommand.PROXY,
            HAProxyProxiedProtocol.TCP4,
            srcAddr,
            dstAddr,
            srcPort,
            dstPort
        );

        EmbeddedChannel channel = new EmbeddedChannel(
            new ProxyProtocolHandler()
        );

        // the haproxy message is terminated ProxyProtocolHandler
        assertFalse(channel.writeInbound(message));

        InetSocketAddress socketAddress = ChannelAttrs.socketAddress(channel);
        assertNotNull(socketAddress);
        assertEquals(socketAddress.getAddress().getHostAddress(), srcAddr);
        assertEquals(socketAddress.getPort(), srcPort);

        assertNull(channel.pipeline().get(ProxyProtocolHandler.class));
    }

    @Test
    public void testNonHAProxyMessagePassthrough() {
        EmbeddedChannel channel = new EmbeddedChannel(
            new ProxyProtocolHandler()
        );

        String testMessage = "normal message";
        assertTrue(channel.writeInbound(testMessage));

        // Should pass through unchanged
        assertEquals(channel.readInbound(), testMessage);

        assertNull(channel.pipeline().get(ProxyProtocolHandler.class));
    }

    @Test
    void testLocalCommand() {
        String srcAddr = "192.168.1.100";
        String dstAddr = "10.0.0.1";
        int srcPort = 54321;
        int dstPort = 1883;
        EmbeddedChannel channel = new EmbeddedChannel(
            new ProxyProtocolHandler(),
            new ClientAddrHandler());

        HAProxyMessage localMessage = new HAProxyMessage(
            HAProxyProtocolVersion.V2,
            HAProxyCommand.LOCAL,
            HAProxyProxiedProtocol.TCP4,
            srcAddr,
            dstAddr,
            srcPort,
            dstPort
        );

        assertFalse(channel.writeInbound(localMessage));

        InetSocketAddress address = ChannelAttrs.socketAddress(channel);
        assertNull(address);
        assertNull(channel.pipeline().get(ClientAddrHandler.class));
    }
}
