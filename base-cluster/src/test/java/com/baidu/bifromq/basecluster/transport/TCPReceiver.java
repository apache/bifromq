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

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TCPReceiver {
    public static void main(String[] args) {
        InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 1111);
        TCPTransport transport = TCPTransport.builder()
            .bindAddr(new InetSocketAddress("127.0.0.1", 2222))
            .opts(new TCPTransport.TCPTransportOptions())
            .build();
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger count = new AtomicInteger(1);
        ByteString.copyFromUtf8(count.incrementAndGet() + "");
        scheduledExecutorService.scheduleAtFixedRate(() ->
                transport.send(Arrays.asList(ByteString.copyFromUtf8(count.incrementAndGet() + "")), remote),
            0, 2, TimeUnit.SECONDS);

        log.info("Start receiving");
        transport.receive()
            .blockingSubscribe(packetEnvelope -> {
                List<ByteString> data = packetEnvelope.data;
                log.info("Data={}, Sender={}", data, packetEnvelope.sender);
            });
    }
}
