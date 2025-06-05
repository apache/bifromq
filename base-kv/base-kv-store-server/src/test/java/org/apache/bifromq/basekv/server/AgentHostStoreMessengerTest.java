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

package org.apache.bifromq.basekv.server;

import static org.apache.bifromq.basekv.Constants.toBaseKVAgentId;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMessage;
import org.apache.bifromq.basecluster.memberlist.agent.IAgent;
import org.apache.bifromq.basecluster.memberlist.agent.IAgentMember;
import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.StoreMessage;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.lang.reflect.Method;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class AgentHostStoreMessengerTest extends MockableTest {
    @Mock
    private IAgentHost agentHost;
    @Mock
    private IAgent agent;

    private String clusterId = "testCluster";
    private String srcStore = "store1";

    @Mock
    private IAgentMember srcStoreAgentMember;
    private KVRangeId srcRange;
    private String targetStore = "store2";
    private PublishSubject<AgentMessage> tgtStoreMessageSubject;
    @Mock
    private IAgentMember tgtStoreAgentMember;
    private KVRangeId targetRange;

    @Override
    protected void doSetup(Method method) {
        tgtStoreMessageSubject = PublishSubject.create();
        srcRange = KVRangeIdUtil.generate();
        targetRange = KVRangeIdUtil.generate();
        when(agentHost.host(toBaseKVAgentId(clusterId))).thenReturn(agent);
        when(agent.register(srcStore)).thenReturn(srcStoreAgentMember);
        when(agent.register(targetStore)).thenReturn(tgtStoreAgentMember);
        when(tgtStoreAgentMember.receive()).thenReturn(tgtStoreMessageSubject);
    }

    @Test
    public void init() {
        when(agentHost.host(toBaseKVAgentId(clusterId))).thenReturn(agent);
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, srcStore);

        ArgumentCaptor<String> agentMemberCap = ArgumentCaptor.forClass(String.class);
        verify(agent).register(agentMemberCap.capture());
        assertEquals(agentMemberCap.getValue(), srcStore);
    }

    @Test
    public void send() {
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, srcStore);
        StoreMessage message = StoreMessage.newBuilder()
            .setFrom(srcStore)
            .setSrcRange(srcRange)
            .setPayload(KVRangeMessage.newBuilder().setHostStoreId(targetStore).setRangeId(targetRange).build())
            .build();
        messenger.send(message);
        ArgumentCaptor<String> targetMemberCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ByteString> msgCap = ArgumentCaptor.forClass(ByteString.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(srcStoreAgentMember).multicast(targetMemberCap.capture(), msgCap.capture(),
            reliableCap.capture());

        assertEquals(targetMemberCap.getValue(), targetStore);
        assertEquals(msgCap.getValue(), message.toByteString());
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void broadcast() {
        when(agentHost.host(toBaseKVAgentId(clusterId))).thenReturn(agent);
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, srcStore);
        StoreMessage message = StoreMessage.newBuilder()
            .setFrom(srcStore)
            .setSrcRange(srcRange)
            .setPayload(KVRangeMessage.newBuilder().setRangeId(targetRange).build())
            .build();
        messenger.send(message);
        ArgumentCaptor<ByteString> msgCap = ArgumentCaptor.forClass(ByteString.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(srcStoreAgentMember).broadcast(msgCap.capture(), reliableCap.capture());
        assertEquals(msgCap.getValue(), message.toByteString());
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void receiveSend() {
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, targetStore);
        TestObserver<StoreMessage> testObserver = TestObserver.create();
        messenger.receive().subscribe(testObserver);

        StoreMessage message = StoreMessage.newBuilder()
            .setFrom(srcStore)
            .setSrcRange(srcRange)
            .setPayload(KVRangeMessage.newBuilder().setHostStoreId(targetStore).setRangeId(targetRange).build())
            .build();
        AgentMessage nodeMessage = AgentMessage.newBuilder()
            .setSender(AgentMemberAddr.newBuilder().setName(srcStore).build())
            .setPayload(message.toByteString())
            .build();
        tgtStoreMessageSubject.onNext(nodeMessage);
        testObserver.awaitCount(1);
        assertEquals(testObserver.values().get(0), message);
    }

    @Test
    public void receiveBroadcast() {
        AgentHostStoreMessenger messenger = new AgentHostStoreMessenger(agentHost, clusterId, targetStore);
        TestObserver<StoreMessage> testObserver = TestObserver.create();
        messenger.receive().subscribe(testObserver);

        StoreMessage message = StoreMessage.newBuilder()
            .setFrom(srcStore)
            .setSrcRange(srcRange)
            .setPayload(KVRangeMessage.newBuilder().setRangeId(targetRange).build())
            .build();
        AgentMessage nodeMessage = AgentMessage.newBuilder()
            .setSender(AgentMemberAddr.newBuilder().setName(srcStore).build())
            .setPayload(message.toByteString())
            .build();
        tgtStoreMessageSubject.onNext(nodeMessage);
        testObserver.awaitCount(1);
        assertEquals(testObserver.values().get(0).getPayload().getHostStoreId(), targetStore);
    }
}
