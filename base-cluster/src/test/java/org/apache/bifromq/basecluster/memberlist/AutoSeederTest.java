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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.messenger.IMessenger;
import org.apache.bifromq.basecluster.messenger.MessageEnvelope;
import org.apache.bifromq.basecluster.proto.ClusterMessage;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class AutoSeederTest {
    @Mock
    private IMessenger messenger;
    @Mock
    private IHostMemberList memberList;
    @Mock
    private IHostAddressResolver addressResolver;
    private Scheduler scheduler = Schedulers.from(Executors.newSingleThreadScheduledExecutor());
    private PublishSubject<Map<HostEndpoint, Integer>> membersSubject = PublishSubject.create();
    private PublishSubject<Timed<MessageEnvelope>> messageSubject = PublishSubject.create();
    private Duration joinTimeout = Duration.ofSeconds(1);
    private Duration joinInterval = Duration.ofMillis(100);
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
        closeable = MockitoAnnotations.openMocks(this);
        when(addressResolver.resolve(REMOTE_HOST_1_ENDPOINT)).thenReturn(REMOTE_ADDR_1);
        when(memberList.local()).thenReturn(LOCAL);
        when(memberList.members()).thenReturn(membersSubject);
    }

    @AfterMethod
    public void releaseMocks(Method method) throws Exception {
        log.info("Test case[{}.{}] finished, doing teardown",
            method.getDeclaringClass().getName(), method.getName());
        closeable.close();
    }

    @Test
    public void join() {
        when(messenger.send(any(ClusterMessage.class), any(InetSocketAddress.class), anyBoolean()))
            .thenReturn(new CompletableFuture<>());
        AutoSeeder seeder =
            new AutoSeeder(messenger, scheduler, memberList, addressResolver, joinTimeout, joinInterval);

        try {
            seeder.join(Collections.singleton(REMOTE_ADDR_1)).join();
            fail();
        } catch (Throwable e) {
            ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
            ArgumentCaptor<InetSocketAddress> addCap = ArgumentCaptor.forClass(InetSocketAddress.class);
            ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);

            verify(messenger, atMost(10)).send(msgCap.capture(), addCap.capture(), reliableCap.capture());

            assertEquals(msgCap.getValue().getJoin().getMember().getEndpoint(), LOCAL_ENDPOINT);
            assertEquals(msgCap.getValue().getJoin().getMember().getIncarnation(), 0);
            assertFalse(msgCap.getValue().getJoin().hasExpectedHost());
            assertEquals(addCap.getValue(), REMOTE_ADDR_1);
            assertTrue(reliableCap.getValue());
        }
    }

    @Test
    public void joinKnownEndpoint() {
        AutoSeeder seeder =
            new AutoSeeder(messenger, scheduler, memberList, addressResolver, joinTimeout, joinInterval);
        membersSubject.onNext(new HashMap<>() {{
            put(REMOTE_HOST_1_ENDPOINT, 0);
        }});
        try {
            seeder.join(Collections.singleton(REMOTE_ADDR_1)).join();
            verify(messenger, atMost(0)).send(any(), any(), anyBoolean());
        } catch (Throwable e) {
            fail();
        }
    }

    @Test
    public void stopJoinEarlier() {
        AutoSeeder seeder =
            new AutoSeeder(messenger, scheduler, memberList, addressResolver, joinTimeout, joinInterval);
        CompletableFuture<Void> joinResult = seeder.join(Collections.singleton(REMOTE_ADDR_1));
        when(messenger.send(any(), any(), anyBoolean())).thenReturn(CompletableFuture.completedFuture(null));
        verify(messenger, timeout(200).atLeast(1)).send(any(), any(), anyBoolean());
        membersSubject.onNext(new HashMap<>() {{
            put(REMOTE_HOST_1_ENDPOINT, 0);
        }});
        joinResult.join();
    }
}
