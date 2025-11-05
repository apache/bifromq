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

package org.apache.bifromq.basecluster.memberlist;

import static java.util.Collections.emptyIterator;
import static org.apache.bifromq.basecluster.memberlist.CRDTUtil.AGENT_HOST_MAP_URI;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.LOCAL_ADDR;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.LOCAL_ENDPOINT;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.LOCAL_REPLICA;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.LOCAL_STORE_ID;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.REMOTE_ADDR_1;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.REMOTE_HOST_1_ENDPOINT;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.ZOMBIE_ENDPOINT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecluster.memberlist.agent.Agent;
import org.apache.bifromq.basecluster.memberlist.agent.IAgent;
import org.apache.bifromq.basecluster.membership.proto.Doubt;
import org.apache.bifromq.basecluster.membership.proto.Fail;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.membership.proto.HostMember;
import org.apache.bifromq.basecluster.membership.proto.Join;
import org.apache.bifromq.basecluster.membership.proto.Quit;
import org.apache.bifromq.basecluster.messenger.IMessenger;
import org.apache.bifromq.basecluster.messenger.MessageEnvelope;
import org.apache.bifromq.basecluster.proto.ClusterMessage;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.store.ICRDTStore;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class HostMemberListTest {
    @Mock
    private IMessenger messenger;
    @Mock
    private ICRDTStore store;
    @Mock
    private IORMap hostListCRDT;
    @Mock
    private IMVReg hostMemberOnCRDT;
    @Mock
    private IHostAddressResolver addressResolver;
    private Scheduler scheduler = Schedulers.from(MoreExecutors.directExecutor());
    private PublishSubject<Long> inflationSubject = PublishSubject.create();
    private PublishSubject<Timed<MessageEnvelope>> messageSubject = PublishSubject.create();
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(store.id()).thenReturn(LOCAL_STORE_ID.toStringUtf8());
        when(store.host(any(), any())).thenReturn(hostListCRDT);
        when(store.host(argThat(uri -> uri.getUri().equals(CRDTUtil.AGENT_HOST_MAP_URI)), any()))
            .thenReturn(hostListCRDT);
        when(hostListCRDT.id()).thenReturn(LOCAL_REPLICA);
        when(hostListCRDT.inflation()).thenReturn(inflationSubject);
        when(messenger.receive()).thenReturn(messageSubject);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void init() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        HostMember local = memberList.local();
        assertEquals(local.getEndpoint().getId(), LOCAL_STORE_ID);
        assertEquals(local.getEndpoint().getAddress(), LOCAL_ADDR.getHostName());
        assertEquals(local.getEndpoint().getPort(), LOCAL_ADDR.getPort());
        assertTrue(local.getIncarnation() >= 0);
        assertTrue(local.getAgentMap().isEmpty());
        assertEquals(memberList.landscape().blockingFirst().size(), 1);
        Map<HostEndpoint, Integer> hostMap = memberList.members().blockingFirst();
        assertEquals(hostMap.size(), 1);
        assertEquals((int) hostMap.get(local.getEndpoint()), local.getIncarnation());
    }

    @Test
    public void host() {
        try (MockedConstruction<Agent> mockAgent = mockConstruction(Agent.class)) {
            String agentId = "agentId";
            when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
            when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
            IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
                messenger, scheduler, store, addressResolver);
            HostMember local = memberList.local();
            IAgent agent = memberList.host(agentId);
            assertEquals(memberList.landscape().blockingFirst().size(), 1);
            assertTrue(memberList.landscape().blockingFirst().get(local.getEndpoint()).contains(agentId));
            assertEquals(mockAgent.constructed().size(), 1);
            assertEquals(memberList.local().getIncarnation(), local.getIncarnation() + 1);
            Map<HostEndpoint, Integer> hostMap = memberList.members().blockingFirst();
            assertEquals(hostMap.size(), 1);
            assertEquals((int) hostMap.get(local.getEndpoint()), local.getIncarnation() + 1);

            verify(hostListCRDT, times(2)).execute(any(ORMapOperation.ORMapUpdate.class));
        }
    }

    @Test
    public void stopHosting() {
        try (MockedConstruction<Agent> mockAgent = mockConstruction(Agent.class)) {
            String agentId = "agentId";
            when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
            when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
            IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
                messenger, scheduler, store, addressResolver);
            memberList.host(agentId);
            when(mockAgent.constructed().get(0).quit()).thenReturn(CompletableFuture.completedFuture(null));
            memberList.stopHosting(agentId);
            assertEquals(memberList.local().getAgentMap().size(), 0);
            assertEquals(memberList.landscape().blockingFirst().size(), 1);
            HostMember local = memberList.local();
            assertEquals(memberList.local().getIncarnation(), local.getIncarnation());
            Map<HostEndpoint, Integer> hostMap = memberList.members().blockingFirst();
            assertEquals((int) hostMap.get(local.getEndpoint()), local.getIncarnation());

            verify(hostListCRDT, times(3)).execute(any(ORMapOperation.ORMapUpdate.class));
        }
    }

    @Test
    public void isZombie() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        assertFalse(memberList.isZombie(memberList.local().getEndpoint()));
        assertTrue(memberList.isZombie(memberList.local()
            .getEndpoint()
            .toBuilder()
            .setId(ByteString.copyFromUtf8("zombie"))
            .build()));
    }

    @Test
    public void handleJoin() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build()));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(1)).execute(opCap.capture());
        verify(store, times(2)).join(
            argThat(r -> r.getUri().equals(AGENT_HOST_MAP_URI) && r.getId().equals(LOCAL_STORE_ID)), any());
    }

    @Test
    public void handleJoinFromNewMemberWithoutExpectedHost() {
        // ensure local crdt read returns empty and allow address resolve
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        when(addressResolver.resolve(REMOTE_HOST_1_ENDPOINT)).thenReturn(REMOTE_ADDR_1);

        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);

        // send a normal join without expectedHost from a new member
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build()));

        // verify one direct join is sent back to the new member
        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addrCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, times(1)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getValue().getJoin().getMember().getEndpoint(), LOCAL_ENDPOINT);
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 0);
        assertEquals(addrCap.getValue(), REMOTE_ADDR_1);
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void handleDuplicatedJoinWithoutExpectedHost() {
        // ensure local crdt read returns empty and allow address resolve
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        when(addressResolver.resolve(REMOTE_HOST_1_ENDPOINT)).thenReturn(REMOTE_ADDR_1);

        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);

        // first join from new member triggers a send-back
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build()));

        // duplicated join from the same member should not trigger another send-back
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build()));

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addrCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, times(1)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getValue().getJoin().getMember().getEndpoint(), LOCAL_ENDPOINT);
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 0);
        assertEquals(addrCap.getValue(), REMOTE_ADDR_1);
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void handleJoinAndClearZombie() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);

        when(hostMemberOnCRDT.read()).thenReturn(Iterators.forArray(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .build().toByteString()));

        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build(), ZOMBIE_ENDPOINT));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        assertEquals(((ORMapOperation.ORMapRemove) opCap.getAllValues().get(1)).valueType, CausalCRDTType.mvreg);
        assertEquals(opCap.getAllValues().get(1).keyPath[0], ZOMBIE_ENDPOINT.toByteString());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(messenger).spread(msgCap.capture());

        assertEquals(msgCap.getValue().getQuit().getEndpoint(), ZOMBIE_ENDPOINT);
        assertEquals(msgCap.getValue().getQuit().getIncarnation(), Integer.MAX_VALUE);
    }

    @Test
    public void handleJoinFromHealing() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        when(addressResolver.resolve(REMOTE_HOST_1_ENDPOINT)).thenReturn(REMOTE_ADDR_1);
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build(), LOCAL_ENDPOINT));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(1)).execute(opCap.capture());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addrCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, times(1)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getValue().getJoin().getMember().getEndpoint(), LOCAL_ENDPOINT);
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 0);
        assertEquals(addrCap.getValue(), REMOTE_ADDR_1);
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void handleJoinFromDuplicatedHealing() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        when(addressResolver.resolve(REMOTE_HOST_1_ENDPOINT)).thenReturn(REMOTE_ADDR_1);
        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build(), LOCAL_ENDPOINT));

        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build(), LOCAL_ENDPOINT));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addrCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);

        verify(messenger, times(2)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getValue().getJoin().getMember().getEndpoint(), LOCAL_ENDPOINT);
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 1);
        assertEquals(addrCap.getValue(), REMOTE_ADDR_1);
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void handleFailAndClearZombie() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(Iterators.forArray(HostMember.newBuilder()
            .setEndpoint(ZOMBIE_ENDPOINT)
            .build().toByteString()));
        messageSubject.onNext(failMsg(ZOMBIE_ENDPOINT, 1));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        assertEquals(((ORMapOperation.ORMapRemove) opCap.getAllValues().get(1)).valueType, CausalCRDTType.mvreg);
        assertEquals(opCap.getAllValues().get(1).keyPath[0], ZOMBIE_ENDPOINT.toByteString());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(messenger).spread(msgCap.capture());

        assertEquals(msgCap.getValue().getQuit().getEndpoint(), ZOMBIE_ENDPOINT);
        assertEquals(msgCap.getValue().getQuit().getIncarnation(), Integer.MAX_VALUE);
    }

    @Test
    public void handleFailAndDrop() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        when(hostMemberOnCRDT.read()).thenReturn(Iterators.forArray(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .build().toByteString()));

        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(1)
            .build()));

        when(hostMemberOnCRDT.read()).thenReturn(Iterators.forArray(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .build().toByteString()));
        messageSubject.onNext(failMsg(REMOTE_HOST_1_ENDPOINT, 1));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        assertEquals(((ORMapOperation.ORMapRemove) opCap.getAllValues().get(1)).valueType, CausalCRDTType.mvreg);
        assertEquals(opCap.getAllValues().get(1).keyPath[0], REMOTE_HOST_1_ENDPOINT.toByteString());
    }

    @Test
    public void handleFailAndRenew() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        assertEquals(memberList.members().blockingFirst().get(LOCAL_ENDPOINT).intValue(), 0);

        messageSubject.onNext(failMsg(LOCAL_ENDPOINT, 0));
        messageSubject.onNext(failMsg(LOCAL_ENDPOINT, 0)); // this time will be ignored

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        assertEquals(opCap.getAllValues().get(1).keyPath[0], LOCAL_ENDPOINT.toByteString());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(messenger).spread(msgCap.capture());
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 1);

        assertEquals(memberList.members().blockingFirst().get(LOCAL_ENDPOINT).intValue(), 1);
    }

    @Test
    public void handleQuitZombie() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());

        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(quitMsg(ZOMBIE_ENDPOINT, 1));
        // nothing will happen
        verify(hostListCRDT, times(0)).execute(any(ORMapOperation.ORMapRemove.class));
        verify(store, times(1)).join(
            argThat(r -> r.getUri().equals(AGENT_HOST_MAP_URI) && r.getId().equals(LOCAL_STORE_ID)), any());
    }

    @Test
    public void handleQuitNotExistMember() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());

        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        when(hostMemberOnCRDT.read()).thenReturn(Iterators.forArray(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .build().toByteString()));

        messageSubject.onNext(quitMsg(REMOTE_HOST_1_ENDPOINT, 1));
        // nothing will happen
        verify(hostListCRDT, never()).execute(any(ORMapOperation.ORMapRemove.class));
        verify(store, times(1)).join(
            argThat(r -> r.getUri().equals(AGENT_HOST_MAP_URI) && r.getId().equals(LOCAL_STORE_ID)), any());
    }

    @Test
    public void handleQuitNotExistMemberOnCRDT() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());

        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());

        messageSubject.onNext(quitMsg(REMOTE_HOST_1_ENDPOINT, 1));
        // nothing will happen
        verify(hostListCRDT, never()).execute(any(ORMapOperation.ORMapRemove.class));
        verify(store, times(1)).join(
            argThat(r -> r.getUri().equals(AGENT_HOST_MAP_URI) && r.getId().equals(LOCAL_STORE_ID)), any());
    }

    @Test
    public void handleQuitSelf() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());

        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(quitMsg(LOCAL_ENDPOINT, 0));
        // nothing will happen
        verify(hostListCRDT, times(0)).execute(any(ORMapOperation.ORMapRemove.class));
        verify(store, times(1)).join(
            argThat(r -> r.getUri().equals(AGENT_HOST_MAP_URI) && r.getId().equals(LOCAL_STORE_ID)), any());
    }

    @Test
    public void handleQuitAndDrop() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());

        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());

        messageSubject.onNext(joinMsg(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .setIncarnation(0)
            .build()));

        when(hostMemberOnCRDT.read()).thenReturn(Iterators.forArray(HostMember.newBuilder()
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .build().toByteString()));
        messageSubject.onNext(quitMsg(REMOTE_HOST_1_ENDPOINT, 0));
        // nothing will happen
        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(1)).execute(opCap.capture());
        assertFalse(opCap.getAllValues().get(0) instanceof ORMapOperation.ORMapRemove);

        verify(store, times(3)).join(
            argThat(r -> r.getUri().equals(AGENT_HOST_MAP_URI) && r.getId().equals(LOCAL_STORE_ID)), any());
    }

    @Test
    public void handleDoubt() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(doubtMsg(LOCAL_ENDPOINT, 0));

        ArgumentCaptor<ORMapOperation> opCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(hostListCRDT, times(2)).execute(opCap.capture());
        assertEquals(opCap.getAllValues().get(1).keyPath[0], LOCAL_ENDPOINT.toByteString());

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        verify(messenger).spread(msgCap.capture());
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 1);
        assertEquals(memberList.members().blockingFirst().get(LOCAL_ENDPOINT).intValue(), 1);
    }

    @Test
    public void handleDoubtAndIgnore() {
        when(hostListCRDT.getMVReg(any())).thenReturn(hostMemberOnCRDT);
        when(hostMemberOnCRDT.read()).thenReturn(emptyIterator());
        IHostMemberList memberList = new HostMemberList(LOCAL_ADDR.getHostName(), LOCAL_ADDR.getPort(),
            messenger, scheduler, store, addressResolver);
        messageSubject.onNext(doubtMsg(REMOTE_HOST_1_ENDPOINT, 0));
        verify(messenger, times(0)).spread(any());
        assertEquals(memberList.members().blockingFirst().get(LOCAL_ENDPOINT).intValue(), 0);
    }

    private Timed<MessageEnvelope> joinMsg(HostMember member) {
        return to(ClusterMessage.newBuilder()
            .setJoin(Join.newBuilder()
                .setMember(member)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> joinMsg(HostMember member, HostEndpoint expected) {
        return to(ClusterMessage.newBuilder()
            .setJoin(Join.newBuilder()
                .setMember(member)
                .setExpectedHost(expected)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> quitMsg(HostEndpoint quitEndpoint, int incarnation) {
        return to(ClusterMessage.newBuilder()
            .setQuit(Quit.newBuilder()
                .setEndpoint(quitEndpoint)
                .setIncarnation(incarnation)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> doubtMsg(HostEndpoint doubtEndpoint, int incarnation) {
        return to(ClusterMessage.newBuilder()
            .setDoubt(Doubt.newBuilder()
                .setEndpoint(doubtEndpoint)
                .setIncarnation(incarnation)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> failMsg(HostEndpoint failedEndpoint, int incarnation) {
        return to(ClusterMessage.newBuilder()
            .setFail(Fail.newBuilder()
                .setEndpoint(failedEndpoint)
                .setIncarnation(incarnation)
                .build())
            .build());
    }

    private Timed<MessageEnvelope> to(ClusterMessage clusterMessage) {
        return new Timed<>(MessageEnvelope.builder()
            .message(clusterMessage)
            .build(), System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }
}
