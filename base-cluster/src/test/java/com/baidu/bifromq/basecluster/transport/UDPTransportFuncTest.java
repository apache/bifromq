/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.basecluster.transport;

import com.baidu.bifromq.basecluster.transport.proto.Packet;
import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class UDPTransportFuncTest {

    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 12345);

    @Test
    public void testSendAndReceive() {
        UDPTransport transport = UDPTransport.builder()
            .env("testEnv")
            .bindAddr(address)
            .build();
        Packet packet = Packet.newBuilder()
            .addMessages(ByteString.copyFrom("test", Charset.defaultCharset()))
            .build();
        transport.send(packet.getMessagesList(), address).join();
        transport.shutdown();
    }
}
