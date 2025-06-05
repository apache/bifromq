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

package org.apache.bifromq.basecluster.messenger;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecluster.messenger.proto.DirectMessage;
import org.apache.bifromq.basecluster.messenger.proto.GossipMessage;
import org.apache.bifromq.basecluster.messenger.proto.MessengerMessage;
import org.apache.bifromq.basecluster.transport.ITransport;
import org.apache.bifromq.basecluster.transport.PacketEnvelope;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MessengerTransportTest {

    @Mock
    ITransport transport;
    private InetSocketAddress address;
    private List<MessengerMessage> messages;
    private List<ByteString> packetData;
    private MessengerTransport messengerTransport;
    private String envA = "envA";
    private String envB = "envB";
    private AutoCloseable closeable;
    @BeforeMethod
    public void init() {
        closeable = MockitoAnnotations.openMocks(this);
        address = new InetSocketAddress("127.0.0.1", 12345);
        MessengerMessage message =
            MessengerMessage.newBuilder()
                .setDirect(
                    DirectMessage.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("test direct"))
                        .build())
                .build();
        MessengerMessage message2 =
            MessengerMessage.newBuilder()
                .setGossip(
                    GossipMessage.newBuilder()
                        .setPayload(ByteString.copyFromUtf8("test gossip"))
                        .build())
                .build();
        messages = new ArrayList<>() {{
            add(message);
            add(message2);
        }};
        packetData = messages.stream().map(msg -> msg.toByteString()).collect(Collectors.toList());
        messengerTransport = new MessengerTransport(transport);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void send() {
        messengerTransport.send(messages, address, false);
        assertFalse(ITransport.RELIABLE.get());
        verify(transport).send(packetData, address);
    }

    @Test
    public void forceSendViaTCP() {
        messengerTransport.send(messages, address, true);
        assertTrue(ITransport.RELIABLE.get());
        verify(transport).send(packetData, address);
    }

    @Test
    public void receive() {
        PublishSubject<PacketEnvelope> packetSubject = PublishSubject.create();
        when(transport.receive()).thenReturn(packetSubject);
        TestObserver<Timed<MessengerMessageEnvelope>> testObserver = new TestObserver<>();
        messengerTransport.receive().subscribe(testObserver);

        packetSubject.onNext(new PacketEnvelope(packetData, address, address));

        testObserver.awaitCount(2);

        assertEquals(testObserver.values().get(0).value().sender, address);
        assertEquals(testObserver.values().get(0).value().recipient, address);
        assertEquals(testObserver.values().get(0).value().message.toByteString(), packetData.get(0));

        assertEquals(testObserver.values().get(1).value().recipient, address);
        assertEquals(testObserver.values().get(1).value().message.toByteString(), packetData.get(1));
    }
}
