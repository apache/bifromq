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

package org.apache.bifromq.basecluster.memberlist.agent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMessage;
import org.apache.bifromq.basecluster.agent.proto.AgentMessageEnvelope;
import org.apache.bifromq.basecluster.memberlist.IHostAddressResolver;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.messenger.IMessenger;
import org.apache.bifromq.basecluster.messenger.MessageEnvelope;
import org.apache.bifromq.basecluster.proto.ClusterMessage;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.net.InetSocketAddress;
import java.rmi.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class AgentMessengerTest {
    private String agentId = "agent";
    private InetSocketAddress srcAddr = new InetSocketAddress("localhost", 1111);
    private InetSocketAddress tgtAddr = new InetSocketAddress("localhost", 2222);
    private HostEndpoint srcEndpoint = HostEndpoint.newBuilder()
        .setId(ByteString.copyFromUtf8("Source"))
        .setAddress(srcAddr.getHostName())
        .setPort(srcAddr.getPort())
        .build();
    private HostEndpoint tgtEndpoint = HostEndpoint.newBuilder()
        .setId(ByteString.copyFromUtf8("Target"))
        .setAddress(tgtAddr.getHostName())
        .setPort(tgtAddr.getPort())
        .build();
    private AgentMemberAddr sourceMemberAddr = AgentMemberAddr.newBuilder()
        .setName("source")
        .setEndpoint(srcEndpoint)
        .build();
    private AgentMemberAddr targetMemberAddr = AgentMemberAddr.newBuilder()
        .setName("target")
        .setEndpoint(tgtEndpoint)
        .build();
    private AgentMessage message = AgentMessage.newBuilder()
        .setSender(sourceMemberAddr)
        .setPayload(ByteString.EMPTY)
        .build();
    @Mock
    private IHostAddressResolver addressResolver;
    @Mock
    private IMessenger messenger;
    private AutoCloseable closeable;
    @BeforeMethod
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }
    @Test
    public void sendToUnknownHost() {
        AgentMessenger agentMessenger = new AgentMessenger(agentId, addressResolver, messenger);
        when(addressResolver.resolve(tgtEndpoint)).thenReturn(null);
        try {
            agentMessenger.send(message, targetMemberAddr, true);
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof UnknownHostException);
        }
    }

    @Test
    public void send() {
        AgentMessenger agentMessenger = new AgentMessenger(agentId, addressResolver, messenger);
        when(addressResolver.resolve(tgtEndpoint)).thenReturn(tgtAddr);
        when(messenger.send(any(), any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
        agentMessenger.send(message, targetMemberAddr, true).join();

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addrCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getValue().getAgentMessage().getAgentId(), agentId);
        assertEquals(msgCap.getValue().getAgentMessage().getMessage(), message);
        assertEquals(msgCap.getValue().getAgentMessage().getReceiver(), targetMemberAddr);
        assertEquals(addrCap.getValue(), tgtAddr);
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void receive() {
        PublishSubject<Timed<MessageEnvelope>> msgSubject = PublishSubject.create();
        when(messenger.receive()).thenReturn(msgSubject);

        TestObserver msgReceiver = new TestObserver();
        AgentMessenger agentMessenger = new AgentMessenger(agentId, addressResolver, messenger);
        agentMessenger.receive().subscribe(msgReceiver);

        MessageEnvelope msg1 = MessageEnvelope.builder().build();

        MessageEnvelope msg2 = MessageEnvelope.builder()
            .sender(srcAddr)
            .message(ClusterMessage.newBuilder()
                .setAgentMessage(AgentMessageEnvelope.newBuilder()
                    .setAgentId("wrong agent")
                    .setReceiver(targetMemberAddr)
                    .setMessage(message)
                    .build())
                .build())
            .build();
        MessageEnvelope msg3 = MessageEnvelope.builder()
            .sender(srcAddr)
            .message(ClusterMessage.newBuilder()
                .setAgentMessage(AgentMessageEnvelope.newBuilder()
                    .setAgentId(agentId)
                    .setReceiver(targetMemberAddr)
                    .setMessage(message)
                    .build())
                .build())
            .build();

        msgSubject.onNext(new Timed<>(msg1, System.currentTimeMillis(), TimeUnit.MILLISECONDS));
        msgSubject.onNext(new Timed<>(msg2, System.currentTimeMillis(), TimeUnit.MILLISECONDS));
        msgSubject.onNext(new Timed<>(msg3, System.currentTimeMillis(), TimeUnit.MILLISECONDS));

        msgReceiver.awaitCount(1);
        assertEquals(message, msg3.message.getAgentMessage().getMessage());
    }
}
