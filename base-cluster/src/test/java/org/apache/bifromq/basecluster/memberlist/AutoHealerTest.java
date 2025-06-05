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

import static org.apache.bifromq.basecluster.memberlist.Fixtures.LOCAL;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.LOCAL_ENDPOINT;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.REMOTE_ADDR_1;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.REMOTE_HOST_1_ENDPOINT;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.ZOMBIE_ENDPOINT;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.endorseMsg;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.failMsg;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.joinMsg;
import static org.apache.bifromq.basecluster.memberlist.Fixtures.quitMsg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.membership.proto.HostMember;
import org.apache.bifromq.basecluster.messenger.IMessenger;
import org.apache.bifromq.basecluster.messenger.MessageEnvelope;
import org.apache.bifromq.basecluster.proto.ClusterMessage;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AutoHealerTest {
    @Mock
    private IMessenger messenger;
    @Mock
    private IHostMemberList memberList;
    @Mock
    private IHostAddressResolver addressResolver;
    private PublishSubject<Map<HostEndpoint, Integer>> membersSubject = PublishSubject.create();
    private PublishSubject<Timed<MessageEnvelope>> messageSubject = PublishSubject.create();
    private Scheduler scheduler = Schedulers.from(Executors.newSingleThreadScheduledExecutor());
    private Duration healingTimeout = Duration.ofSeconds(1);
    private Duration healingInterval = Duration.ofMillis(100);
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(addressResolver.resolve(REMOTE_HOST_1_ENDPOINT)).thenReturn(REMOTE_ADDR_1);
        when(memberList.local()).thenReturn(LOCAL);
        when(messenger.receive()).thenReturn(messageSubject);
        when(memberList.members()).thenReturn(membersSubject);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void init() {
        AutoHealer healer =
            new AutoHealer(messenger, scheduler, memberList, addressResolver, healingTimeout, healingInterval);
    }

    @Test
    public void healTimeout() {
        AutoHealer healer =
            new AutoHealer(messenger, scheduler, memberList, addressResolver, healingTimeout, healingInterval);
        membersSubject.onNext(new HashMap<>() {{
            put(LOCAL_ENDPOINT, 0);
            put(REMOTE_HOST_1_ENDPOINT, 0);
        }});
        messageSubject.onNext(failMsg(REMOTE_HOST_1_ENDPOINT, 0));

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, timeout(1500).atLeast(9)).send(msgCap.capture(), addCap.capture(), reliableCap.capture());
        assertTrue(msgCap.getAllValues().size() <= 10);
    }

    @Test
    public void handleFail() {
        AutoHealer healer =
            new AutoHealer(messenger, scheduler, memberList, addressResolver, healingTimeout, healingInterval);
        membersSubject.onNext(new HashMap<>() {{
            put(LOCAL_ENDPOINT, 0);
            put(REMOTE_HOST_1_ENDPOINT, 0);
        }});
        messageSubject.onNext(failMsg(REMOTE_HOST_1_ENDPOINT, 0));

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, timeout(1500).atLeast(1)).send(msgCap.capture(), addCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getValue().getJoin().getMember().getEndpoint(), LOCAL_ENDPOINT);
        assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 0);
        assertEquals(addCap.getValue(), REMOTE_ADDR_1);
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void handleIgnoreFail() {
        AutoHealer healer =
            new AutoHealer(messenger, scheduler, memberList, addressResolver, healingTimeout, healingInterval);
        messageSubject.onNext(failMsg(LOCAL_ENDPOINT, 0));

        assertNoMoreHealingMessages();
    }

    @Test
    public void handleZombieFail() {
        AutoHealer healer =
            new AutoHealer(messenger, scheduler, memberList, addressResolver, healingTimeout, healingInterval);
        messageSubject.onNext(failMsg(ZOMBIE_ENDPOINT, 0));

        assertNoMoreHealingMessages();
    }

    @Test
    public void handleNotKnownFail() {
        AutoHealer healer =
            new AutoHealer(messenger, scheduler, memberList, addressResolver, healingTimeout, healingInterval);
        messageSubject.onNext(failMsg(REMOTE_HOST_1_ENDPOINT, 0));

        assertNoMoreHealingMessages();
    }

    @Test
    public void handleObsoleteFail() {
        AutoHealer healer =
            new AutoHealer(messenger, scheduler, memberList, addressResolver, healingTimeout, healingInterval);
        membersSubject.onNext(new HashMap<>() {{
            put(LOCAL_ENDPOINT, 0);
            put(REMOTE_HOST_1_ENDPOINT, 1); // newer incarnation
        }});

        messageSubject.onNext(failMsg(REMOTE_HOST_1_ENDPOINT, 0));

        assertNoMoreHealingMessages();
    }

    @SneakyThrows
    @Test
    public void handleJoin() {
        handleHealingMsg(joinMsg(HostMember.newBuilder()
            .setIncarnation(1)
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .build()));
        reset(messenger);
        Thread.sleep(500);
        verify(messenger, never()).send(any(), any(), anyBoolean());
    }

    @SneakyThrows
    @Test
    public void handleObsoleteJoin() {
        handleObsoleteHealingMsg(joinMsg(HostMember.newBuilder()
            .setIncarnation(0) // older incarnation
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .build()));
        handleObsoleteHealingMsg(joinMsg(HostMember.newBuilder()
            .setIncarnation(1) // same incarnation
            .setEndpoint(REMOTE_HOST_1_ENDPOINT)
            .build()));
        reset(messenger);
        Thread.sleep(500);
        verify(messenger, atLeastOnce()).send(any(), any(), anyBoolean());
    }

    @Test
    public void handleQuit() {
        handleHealingMsg(quitMsg(REMOTE_HOST_1_ENDPOINT, 1));
    }

    @SneakyThrows
    @Test
    public void handleObsoleteQuit() {
        handleObsoleteHealingMsg(quitMsg(REMOTE_HOST_1_ENDPOINT, 0));
        reset(messenger);
        Thread.sleep(500);
        verify(messenger, atLeastOnce()).send(any(), any(), anyBoolean());
    }

    @Test
    public void handleEndorse() {
        handleHealingMsg(endorseMsg(REMOTE_HOST_1_ENDPOINT, 1));
    }

    @SneakyThrows
    @Test
    public void handleObsoleteEndorse() {
        handleObsoleteHealingMsg(endorseMsg(REMOTE_HOST_1_ENDPOINT, 0));
        reset(messenger);
        Thread.sleep(500);
        verify(messenger, atLeastOnce()).send(any(), any(), anyBoolean());
    }

    private void handleHealingMsg(Timed<MessageEnvelope> msg) {
        AutoHealer healer =
            new AutoHealer(messenger, scheduler, memberList, addressResolver, Duration.ofSeconds(20), healingInterval);
        membersSubject.onNext(new HashMap<>() {{
            put(LOCAL_ENDPOINT, 0);
            put(REMOTE_HOST_1_ENDPOINT, 0);
        }});
        messageSubject.onNext(failMsg(REMOTE_HOST_1_ENDPOINT, 0));

        verify(messenger, timeout(200).atLeast(1)).send(any(), any(), anyBoolean());

        messageSubject.onNext(msg);

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, timeout(2500).atLeast(0)).send(msgCap.capture(), addCap.capture(), reliableCap.capture());
        assertTrue(msgCap.getAllValues().size() < 9);
    }

    private void handleObsoleteHealingMsg(Timed<MessageEnvelope> msg) {
        AutoHealer healer =
            new AutoHealer(messenger, scheduler, memberList, addressResolver, Duration.ofSeconds(20), healingInterval);
        membersSubject.onNext(new HashMap<>() {{
            put(LOCAL_ENDPOINT, 0);
            put(REMOTE_HOST_1_ENDPOINT, 1);
        }});
        messageSubject.onNext(failMsg(REMOTE_HOST_1_ENDPOINT, 1));

        messageSubject.onNext(msg);

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, timeout(1500).atLeast(9)).send(msgCap.capture(), addCap.capture(), reliableCap.capture());
        assertTrue(msgCap.getAllValues().size() <= 10);
    }

    private void assertNoMoreHealingMessages() {
        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, timeout(1500).atLeast(0)).send(msgCap.capture(), addCap.capture(), reliableCap.capture());
        assertTrue(msgCap.getAllValues().isEmpty());
    }
}
