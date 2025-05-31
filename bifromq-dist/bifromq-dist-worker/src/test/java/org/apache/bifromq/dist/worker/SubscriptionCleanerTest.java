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

package org.apache.bifromq.dist.worker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.plugin.subbroker.ISubBroker;
import org.apache.bifromq.plugin.subbroker.ISubBrokerManager;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubscriptionCleanerTest {
    @Mock
    private ISubBrokerManager subBrokerManager;
    @Mock
    private IDistClient distClient;
    @Mock
    private ISubBroker subBroker;
    private SubscriptionCleaner subscriptionCleaner;
    private AutoCloseable openMocks;

    @BeforeMethod
    void setUp() {
        openMocks = openMocks(this);
        subscriptionCleaner = new SubscriptionCleaner(subBrokerManager, distClient);
    }

    @AfterMethod
    void tearDown() throws Exception {
        openMocks.close();
    }

    @Test
    void testSweepNoSub() {
        int subBrokerId = 1;
        String tenantId = "tenant1";
        RouteMatcher matcher = TopicUtil.from("topic1");
        String receiverId = "receiver1";
        String delivererKey = "deliverer1";

        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(matcher)
            .setReceiverId(receiverId)
            .setIncarnation(1)
            .build();
        CheckRequest request = CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(delivererKey)
            .addMatchInfo(matchInfo)
            .build();

        CheckReply checkReply = CheckReply.newBuilder()
            .addCode(CheckReply.Code.NO_SUB)
            .build();

        when(subBrokerManager.get(subBrokerId)).thenReturn(subBroker);
        when(subBroker.check(request)).thenReturn(
            CompletableFuture.completedFuture(checkReply));
        when(distClient.removeRoute(anyLong(), eq(tenantId), eq(matcher), eq(receiverId), eq(delivererKey),
            eq(subBrokerId), eq(matchInfo.getIncarnation())))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));

        subscriptionCleaner.sweep(subBrokerId, request).join();

        verify(subBrokerManager, times(1)).get(subBrokerId);
        verify(subBroker, times(1)).check(request);
        verify(distClient, times(1)).removeRoute(anyLong(), eq(tenantId), eq(matcher), eq(receiverId),
            eq(delivererKey), eq(subBrokerId), eq(matchInfo.getIncarnation()));
    }

    @Test
    void testSweepNoReceiver() {
        int subBrokerId = 1;
        String tenantId = "tenant1";
        RouteMatcher matcher = TopicUtil.from("topic1");
        String receiverId = "receiver1";
        String delivererKey = "deliverer1";

        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(matcher)
            .setReceiverId(receiverId)
            .setIncarnation(1)
            .build();
        CheckRequest request = CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(delivererKey)
            .addMatchInfo(matchInfo)
            .build();

        CheckReply checkReply = CheckReply.newBuilder()
            .addCode(CheckReply.Code.NO_RECEIVER)
            .build();

        when(subBrokerManager.get(subBrokerId)).thenReturn(subBroker);
        when(subBroker.check(request)).thenReturn(
            CompletableFuture.completedFuture(checkReply));
        when(distClient.removeRoute(anyLong(), eq(tenantId), eq(matcher), eq(receiverId), eq(delivererKey),
            eq(subBrokerId), eq(matchInfo.getIncarnation())))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));

        subscriptionCleaner.sweep(subBrokerId, request).join();

        verify(subBrokerManager, times(1)).get(subBrokerId);
        verify(subBroker, times(1)).check(request);
        verify(distClient, times(1)).removeRoute(anyLong(), eq(tenantId), eq(matcher), eq(receiverId),
            eq(delivererKey), eq(subBrokerId), eq(matchInfo.getIncarnation()));
    }

    @Test
    void testSweepNoAction() {
        int subBrokerId = 1;
        String tenantId = "tenant1";
        RouteMatcher matcher = TopicUtil.from("topic1");
        String receiverId = "receiver1";
        String delivererKey = "deliverer1";

        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(matcher)
            .setReceiverId(receiverId)
            .setIncarnation(1)
            .build();
        CheckRequest request = CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(delivererKey)
            .addMatchInfo(matchInfo)
            .build();

        CheckReply checkReply = CheckReply.newBuilder()
            .addCode(CheckReply.Code.OK)
            .build();

        when(subBrokerManager.get(subBrokerId)).thenReturn(subBroker);
        when(subBroker.check(request)).thenReturn(
            CompletableFuture.completedFuture(checkReply));

        subscriptionCleaner.sweep(subBrokerId, request).join();

        verify(subBrokerManager, times(1)).get(subBrokerId);
        verify(subBroker, times(1)).check(request);
        verify(distClient, times(0)).removeRoute(anyLong(), anyString(), any(), anyString(), anyString(),
            anyInt(), anyLong());
    }
}