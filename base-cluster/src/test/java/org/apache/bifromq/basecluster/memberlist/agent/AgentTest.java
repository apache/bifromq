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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.bifromq.basecluster.agent.proto.AgentEndpoint;
import org.apache.bifromq.basecluster.agent.proto.AgentMember;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import org.apache.bifromq.basecluster.agent.proto.AgentMessageEnvelope;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.ICRDTStore;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class AgentTest {
    private String agentId = "agentA";
    private ByteString hostId = ByteString.copyFromUtf8("host1");
    private AgentEndpoint endpoint1 = AgentEndpoint.newBuilder()
        .setEndpoint(HostEndpoint.newBuilder()
            .setId(hostId)
            .setAddress("localhost")
            .setPort(1111)
            .build())
        .build();
    private AgentEndpoint endpoint2 = AgentEndpoint.newBuilder()
        .setEndpoint(HostEndpoint.newBuilder()
            .setId(hostId)
            .setAddress("localhost")
            .setPort(2222)
            .build())
        .build();
    private Replica replica;
    @Mock
    private IAgentMessenger agentMessenger;
    private Scheduler scheduler; // make test more deterministic
    @Mock
    private ICRDTStore crdtStore;
    @Mock
    private IAgentAddressProvider hostProvider;
    @Mock
    private IORMap orMap;
    private PublishSubject<Long> inflationSubject;
    private PublishSubject<Set<AgentEndpoint>> agentEndpointsSubject;
    private PublishSubject<AgentMessageEnvelope> messageSubject;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        scheduler = Schedulers.from(MoreExecutors.directExecutor());
        inflationSubject = PublishSubject.create();
        agentEndpointsSubject = PublishSubject.create();
        messageSubject = PublishSubject.create();
        replica = Replica.newBuilder()
            .setUri(CRDTUtil.toAgentURI(agentId))
            .setId(endpoint1.toByteString())
            .build();
        when(crdtStore.host(replica, endpoint1.toByteString())).thenReturn(orMap);
        when(orMap.execute(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(orMap.id()).thenReturn(replica);
        when(orMap.inflation()).thenReturn(inflationSubject);
        when(hostProvider.agentAddress()).thenReturn(agentEndpointsSubject);
        when(agentMessenger.receive()).thenReturn(messageSubject);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void init() {
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        assertEquals(agent.id(), agentId);
        assertEquals(agent.local(), endpoint1);
    }

    @SneakyThrows
    @Test
    public void register() {
        String agentMemberName = "agentMember1";
        ByteString meta1 = ByteString.EMPTY;
        ByteString meta2 = ByteString.copyFromUtf8("Hello");

        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        IAgentMember agentMember = agent.register(agentMemberName);
        assertEquals(agentMember.address().getName(), agentMemberName);
        assertEquals(agentMember.address().getEndpoint(), endpoint1.getEndpoint());

        ArgumentCaptor<ORMapOperation> orMapOpCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(orMap, times(1)).execute(orMapOpCap.capture());
        ORMapOperation op = orMapOpCap.getAllValues().get(0);
        assertTrue(op instanceof ORMapOperation.ORMapUpdate);
        assertTrue(((ORMapOperation.ORMapUpdate) op).valueOp instanceof MVRegOperation);
        AgentMemberAddr key = AgentMemberAddr.parseFrom(op.keyPath[0]);
        assertEquals(key.getName(), agentMemberName);
        assertEquals(key.getEndpoint(), endpoint1.getEndpoint());

        AgentMemberMetadata agentMemberMetadata1 =
            AgentMemberMetadata.parseFrom(((MVRegOperation) ((ORMapOperation.ORMapUpdate) op).valueOp).value);
        assertEquals(agentMemberMetadata1.getValue(), meta1);
        assertTrue(agentMemberMetadata1.getHlc() > 0);
    }

    @Test
    public void deregister() {
        String agentMemberName = "agentMember1";
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        IAgentMember agentMember = agent.register(agentMemberName);
        agent.deregister(agentMember).join();
        // nothing should happen
        agent.deregister(agentMember).join();

        // register again should return distinct object
        IAgentMember newAgentMember = agent.register(agentMemberName);
        assertNotEquals(newAgentMember, agentMember);
    }

    @Test
    public void membership() {
        String agentMemberName = "agentMember1";
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        TestObserver<Map<AgentMemberAddr, AgentMemberMetadata>> testObserver = new TestObserver<>();
        agent.membership().subscribe(testObserver);
        IAgentMember agentMember = agent.register(agentMemberName);
        AgentMember member = AgentMember.newBuilder()
            .setAddr(AgentMemberAddr.newBuilder().setName(agentId).setEndpoint(endpoint1.getEndpoint())
                .setIncarnation(endpoint1.getIncarnation()).build())
            .setMetadata(AgentMemberMetadata.newBuilder().setValue(ByteString.EMPTY).build())
            .build();
        MockUtil.mockAgentMemberCRDT(orMap, Collections.singletonMap(
            MockUtil.toAgentMemberAddr(agentMemberName, endpoint1),
            MockUtil.toAgentMemberMetadata(ByteString.EMPTY)));
        inflationSubject.onNext(System.currentTimeMillis());
        testObserver.awaitCount(2);
        assertEquals(testObserver.values().get(1).size(), 1);
    }

    @Test
    public void hostUpdate() {
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        MockUtil.mockAgentMemberCRDT(orMap, Collections.emptyMap());
        Set<AgentEndpoint> endpoints = Sets.newHashSet(endpoint1, endpoint2);
        agentEndpointsSubject.onNext(endpoints);

        verify(crdtStore).join(replica,
            endpoints.stream().map(AgentEndpoint::toByteString).collect(Collectors.toSet()));
    }

    @SneakyThrows
    @Test
    public void dropMemberWhileHostUpdate() {
        String agentMember1 = "agentMember1";
        String agentMember2 = "agentMember2";
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        Map<AgentMemberAddr, AgentMemberMetadata> members = new HashMap<>();
        members.put(MockUtil.toAgentMemberAddr(agentMember1, endpoint1),
            MockUtil.toAgentMemberMetadata(ByteString.EMPTY));
        members.put(MockUtil.toAgentMemberAddr(agentMember2, endpoint2),
            MockUtil.toAgentMemberMetadata(ByteString.EMPTY));
        MockUtil.mockAgentMemberCRDT(orMap, members);
        Set<AgentEndpoint> endpoints = Sets.newHashSet(endpoint1, endpoint2);
        agentEndpointsSubject.onNext(endpoints);
        // mock ormap again
        MockUtil.mockAgentMemberCRDT(orMap, members);
        endpoints = Sets.newHashSet(endpoint1);
        agentEndpointsSubject.onNext(endpoints);

        ArgumentCaptor<ORMapOperation> orMapOpCap = ArgumentCaptor.forClass(ORMapOperation.class);
        verify(orMap).execute(orMapOpCap.capture());
        ORMapOperation op = orMapOpCap.getValue();
        assertTrue(op instanceof ORMapOperation.ORMapRemove);
        assertEquals(((ORMapOperation.ORMapRemove) op).valueType, CausalCRDTType.mvreg);
        AgentMemberAddr key = AgentMemberAddr.parseFrom(op.keyPath[0]);
        assertEquals(key.getName(), agentMember2);
        assertEquals(key.getEndpoint(), endpoint2.getEndpoint());

        verify(crdtStore).join(replica, endpoints.stream().map(AgentEndpoint::toByteString).collect(
            Collectors.toSet()));
    }

    @Test
    public void quit() {
        Agent agent = new Agent(agentId, endpoint1, agentMessenger, scheduler, crdtStore, hostProvider);
        IAgentMember agentMember = agent.register("agentMember");
        when(crdtStore.stopHosting(replica)).thenReturn(CompletableFuture.completedFuture(null));
        TestObserver membersObserver = new TestObserver();
        agent.membership().subscribe(membersObserver);
        agent.quit().join();
        membersObserver.assertComplete();
        try {
            agentMember.broadcast(ByteString.copyFromUtf8("hello"), false);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }
        // nothing should happen
        agent.quit().join();
    }
}
