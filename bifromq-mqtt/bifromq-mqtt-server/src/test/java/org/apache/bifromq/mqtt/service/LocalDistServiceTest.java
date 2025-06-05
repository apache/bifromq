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

package org.apache.bifromq.mqtt.service;

import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientFanOutBytesPerSeconds;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.bifromq.mqtt.inbox.util.DelivererKeyUtil.toDelivererKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.session.IMQTTTransientSession;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryPackage;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.plugin.subbroker.DeliveryResults;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.TopicUtil;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class LocalDistServiceTest extends MockableTest {
    private final String serverId = "serverId";
    @Mock
    IResourceThrottler resourceThrottler;
    @Mock
    ILocalTopicRouter localTopicRouter;
    LocalDistService localDistService;
    @Mock
    private ILocalSessionRegistry localSessionRegistry;
    @Mock
    private IDistClient distClient;

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) {
        super.setup(method);
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);
    }

    @Test
    public void checkMatchInfoForSharedSub() {
        String topicFilter = "$share/group/topicFilter";
        String tenantId = "tenantId";
        String channelId = "channelId";

        CheckReply.Code code = localDistService.checkMatchInfo(tenantId, MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId(ILocalDistService.globalize(channelId))
            .build());
        assertEquals(code, CheckReply.Code.NO_RECEIVER);

        IMQTTTransientSession session = mock(IMQTTTransientSession.class);
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(session.clientInfo()).thenReturn(clientInfo);
        when(session.channelId()).thenReturn(channelId);
        when(session.hasSubscribed(topicFilter)).thenReturn(true);
        when(localSessionRegistry.get(channelId)).thenReturn(session);
        long reqId = System.nanoTime();
        localDistService.match(reqId, topicFilter, 1, session);
        code = localDistService.checkMatchInfo(tenantId, MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId(ILocalDistService.globalize(channelId))
            .build());
        assertEquals(code, CheckReply.Code.OK);
    }

    @Test
    public void checkMatchInfoForNonSharedSub() {
        String tenantId = "tenantId";
        String topicFilter = "topicFilter";
        String channelId = "channelId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        IMQTTTransientSession session = mock(IMQTTTransientSession.class);
        when(session.clientInfo()).thenReturn(clientInfo);
        when(session.channelId()).thenReturn(channelId);

        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(Optional.empty());
        CheckReply.Code code = localDistService.checkMatchInfo(tenantId, MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId(ILocalDistService.localize(channelId))
            .build());
        assertEquals(code, CheckReply.Code.NO_RECEIVER);

        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(Optional.of(new CompletableFuture<>()));
        code = localDistService.checkMatchInfo(tenantId, MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId(ILocalDistService.localize(channelId))
            .build());
        assertEquals(code, CheckReply.Code.OK);

        ILocalTopicRouter.ILocalRoutes localRoutes = mock(ILocalTopicRouter.ILocalRoutes.class);
        when(localRoutes.localReceiverId()).thenReturn("fakeReceiver");
        when(localTopicRouter.getTopicRoutes(anyString(), any()))
            .thenReturn(Optional.of(CompletableFuture.completedFuture(localRoutes)));
        code = localDistService.checkMatchInfo(tenantId, MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId(ILocalDistService.localize(channelId))
            .build());
        assertEquals(code, CheckReply.Code.NO_RECEIVER);

        localRoutes = mock(ILocalTopicRouter.ILocalRoutes.class);
        when(localRoutes.localReceiverId()).thenReturn(ILocalDistService.localize(channelId));
        when(localTopicRouter.getTopicRoutes(anyString(), any()))
            .thenReturn(Optional.of(CompletableFuture.completedFuture(localRoutes)));
        code = localDistService.checkMatchInfo(tenantId, MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId(ILocalDistService.localize(channelId))
            .build());
        assertEquals(code, CheckReply.Code.NO_RECEIVER);

        when(localRoutes.routesInfo()).thenReturn(Map.of(channelId, 1L));
        code = localDistService.checkMatchInfo(tenantId, MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId(ILocalDistService.localize(channelId))
            .build());
        assertEquals(code, CheckReply.Code.OK);
    }

    @Test
    public void matchSharedSubTopicFilter() {
        String topicFilter = "$share/group/topicFilter";
        for (int i = 0; i < 100; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            String tenantId = "tenantId" + i;
            String channelId = "channelId" + i;
            ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn(channelId);
            long reqId = System.nanoTime();
            localDistService.match(reqId, topicFilter, 1L, session);
            verify(distClient)
                .addRoute(eq(reqId), eq(tenantId), eq(TopicUtil.from(topicFilter)),
                    eq(ILocalDistService.globalize(channelId)),
                    eq(toDelivererKey(tenantId, ILocalDistService.globalize(channelId), serverId)), eq(0), eq(1L));
            reset(distClient);
        }
    }

    @Test
    public void unmatchSharedSubTopicFilter() {
        String topicFilter = "$share/group/topicFilter";
        for (int i = 0; i < 100; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            String tenantId = "tenantId" + i;
            String channelId = "channelId" + i;
            ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn(channelId);
            when(session.hasSubscribed(topicFilter)).thenReturn(false);
            when(localSessionRegistry.get(channelId)).thenReturn(session);
            long reqId = System.nanoTime();
            localDistService.unmatch(reqId, topicFilter, 1L, session);
            verify(distClient).removeRoute(eq(reqId), eq(tenantId), eq(TopicUtil.from(topicFilter)),
                eq(ILocalDistService.globalize(channelId)),
                eq(toDelivererKey(tenantId, ILocalDistService.globalize(channelId), serverId)), eq(0), eq(1L));
            CheckReply.Code code = localDistService.checkMatchInfo(tenantId, MatchInfo.newBuilder()
                .setMatcher(TopicUtil.from(topicFilter))
                .setReceiverId(ILocalDistService.globalize(channelId))
                .build());
            assertEquals(code, CheckReply.Code.NO_SUB);
            reset(distClient);
        }
    }

    @Test
    public void matchSameNonSharedTopicFilter() {
        String tenantId = "tenantId";
        String topicFilter = "topicFilter";
        String channelId = "channelId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        IMQTTTransientSession session = mock(IMQTTTransientSession.class);
        when(session.clientInfo()).thenReturn(clientInfo);
        when(session.channelId()).thenReturn(channelId);
        localDistService.match(System.nanoTime(), topicFilter, 1L, session);
        verify(localTopicRouter).addTopicRoute(anyLong(), eq(tenantId), eq(topicFilter), eq(1L), eq(channelId));
    }

    @Test
    public void unmatchSameNonSharedTopicFilter() {
        String topicFilter = "topicFilter";
        String tenantId = "tenantId";
        String channelId = "channelId";
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        long reqId = System.nanoTime();
        IMQTTTransientSession session = mock(IMQTTTransientSession.class);
        when(session.clientInfo()).thenReturn(clientInfo);
        when(session.channelId()).thenReturn(channelId);
        localDistService.unmatch(reqId, topicFilter, 1L, session);
        verify(localTopicRouter).removeTopicRoute(anyLong(), eq(tenantId), eq(topicFilter), eq(1L), eq(channelId));
    }

    @Test
    public void sharedSubMatchingAndDist() {
        // Setup the local distribution service
        LocalDistService localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);

        // Define the shared subscription topic filter
        String topicFilter = "$share/group/sensor/data";
        String tenantId = "tenantId";
        long reqId = System.nanoTime();
        int numberOfSessions = 5;
        List<IMQTTTransientSession> sessions = new ArrayList<>();

        // Mock client info and sessions
        for (int i = 0; i < numberOfSessions; i++) {
            IMQTTTransientSession session = mock(IMQTTTransientSession.class);
            ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
            when(session.clientInfo()).thenReturn(clientInfo);
            when(session.channelId()).thenReturn("channelId" + i);
            when(session.publish(any(), any())).thenReturn(Collections.emptySet());
            when(localSessionRegistry.get("channelId" + i)).thenReturn(session);
            sessions.add(session);
            localDistService.match(reqId, topicFilter, 1L, session);
        }

        // Prepare delivery request and distribute messages
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId(ILocalDistService.globalize("channelId0"))
            .build();
        DeliveryPack pack =
            DeliveryPack.newBuilder().setMessagePack(TopicMessagePack.newBuilder().build()).addMatchInfo(matchInfo)
                .build();
        DeliveryPackage deliveryPackage = DeliveryPackage.newBuilder().addPack(pack).build();
        DeliveryRequest request = DeliveryRequest.newBuilder().putPackage(tenantId, deliveryPackage).build();

        // Call the distribution method and get the reply
        CompletableFuture<DeliveryReply> futureReply = localDistService.dist(request);
        DeliveryReply reply = futureReply.join();

        // Validate the results
        assertEquals(reply.getResultMap().get(tenantId).getResultList().size(), 1);
        assertTrue(reply.getResultMap().get(tenantId).getResultList().stream()
            .allMatch(result -> result.getCode() == DeliveryResult.Code.OK));

        // Verify that the publish method was called correctly
        verify(sessions.get(0), times(1)).publish(any(), any());
    }

    @Test
    public void deliverToLocalRoute() {
        String tenantId = "tenant1";
        String topic = "testTopic";
        String topicFilter = "testTopic/#";
        String channelId = "channel0";
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId("receiverId").build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).build();
        DeliveryPackage deliveryPack = DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(topicMessagePack).addMatchInfo(matchInfo).build())
            .build();
        DeliveryRequest request = DeliveryRequest.newBuilder().putPackage(tenantId, deliveryPack).build();

        IMQTTTransientSession mockTransientSession = mock(IMQTTTransientSession.class);
        when(mockTransientSession.channelId()).thenReturn(channelId);
        when(mockTransientSession.publish(any(), any())).thenReturn(Collections.emptySet());
        when(localSessionRegistry.get(anyString())).thenReturn(mockTransientSession);

        ILocalTopicRouter.ILocalRoutes localRoutes = mock(ILocalTopicRouter.ILocalRoutes.class);
        when(localRoutes.localReceiverId()).thenReturn("receiverId");
        when(localRoutes.routesInfo()).thenReturn(Map.of(channelId, 1L));
        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(
            Optional.of(CompletableFuture.completedFuture(localRoutes)));

        LocalDistService localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);

        CompletableFuture<DeliveryReply> future = localDistService.dist(request);
        DeliveryReply reply = future.join();

        verify(localSessionRegistry).get(anyString());
        verify(mockTransientSession).publish(any(), any());

        assertNotNull(reply);
        DeliveryResults results = reply.getResultMap().get(tenantId);
        assertNotNull(results);
        DeliveryResult result = results.getResult(0);
        assertEquals(result.getCode(), DeliveryResult.Code.OK);
    }

    @Test
    public void deliverToMismatchedReceiver() {
        String tenantId = "tenant1";
        String topic = "testTopic";
        String topicFilter = "testTopic/#";
        String channelId = "channel0";
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId("receiverIdA")
            .build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).build();
        DeliveryPackage deliveryPack = DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(topicMessagePack).addMatchInfo(matchInfo).build())
            .build();
        DeliveryRequest request = DeliveryRequest.newBuilder().putPackage(tenantId, deliveryPack).build();

        IMQTTTransientSession mockTransientSession = mock(IMQTTTransientSession.class);
        when(mockTransientSession.channelId()).thenReturn(channelId);
        when(mockTransientSession.publish(any(), any())).thenReturn(Collections.emptySet());
        when(localSessionRegistry.get(anyString())).thenReturn(mockTransientSession);

        ILocalTopicRouter.ILocalRoutes localRoutes = mock(ILocalTopicRouter.ILocalRoutes.class);
        when(localRoutes.localReceiverId()).thenReturn("receiverIdB");
        when(localRoutes.routesInfo()).thenReturn(Map.of(channelId, 1L));
        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(
            Optional.of(CompletableFuture.completedFuture(localRoutes)));

        LocalDistService localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);

        CompletableFuture<DeliveryReply> future = localDistService.dist(request);
        DeliveryReply reply = future.join();

        DeliveryResults results = reply.getResultMap().get(tenantId);
        DeliveryResult result = results.getResult(0);
        assertEquals(DeliveryResult.Code.NO_SUB, result.getCode());
    }

    @Test
    public void deliverToNoLocalRoute() {
        String tenantId = "tenant1";
        String topic = "testTopic";
        String topicFilter = "testTopic/#";
        String channelId = "channel0";
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId("receiverId")
            .build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).build();
        DeliveryPackage deliveryPack = DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(topicMessagePack).addMatchInfo(matchInfo).build())
            .build();
        DeliveryRequest request = DeliveryRequest.newBuilder().putPackage(tenantId, deliveryPack).build();

        IMQTTTransientSession mockTransientSession = mock(IMQTTTransientSession.class);
        when(mockTransientSession.channelId()).thenReturn(channelId);
        when(mockTransientSession.publish(any(), any())).thenReturn(Collections.emptySet());
        when(localSessionRegistry.get(anyString())).thenReturn(mockTransientSession);

        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(Optional.empty());

        LocalDistService localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);

        CompletableFuture<DeliveryReply> future = localDistService.dist(request);
        DeliveryReply reply = future.join();

        DeliveryResults results = reply.getResultMap().get(tenantId);
        DeliveryResult result = results.getResult(0);
        assertEquals(DeliveryResult.Code.NO_SUB, result.getCode());
    }

    @Test
    public void deliverToNoResolvedRoute() {
        String tenantId = "tenant1";
        String topic = "testTopic";
        String topicFilter = "testTopic/#";
        String channelId = "channel0";
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId("receiverId")
            .build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).build();
        DeliveryPackage deliveryPack = DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(topicMessagePack).addMatchInfo(matchInfo).build())
            .build();
        DeliveryRequest request = DeliveryRequest.newBuilder().putPackage(tenantId, deliveryPack).build();

        IMQTTTransientSession mockTransientSession = mock(IMQTTTransientSession.class);
        when(mockTransientSession.channelId()).thenReturn(channelId);
        when(mockTransientSession.publish(any(), any())).thenReturn(Collections.emptySet());
        when(localSessionRegistry.get(anyString())).thenReturn(mockTransientSession);

        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(Optional.of(new CompletableFuture<>()));

        LocalDistService localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);

        CompletableFuture<DeliveryReply> future = localDistService.dist(request);
        DeliveryReply reply = future.join();

        DeliveryResults results = reply.getResultMap().get(tenantId);
        DeliveryResult result = results.getResult(0);
        assertEquals(DeliveryResult.Code.OK, result.getCode());
    }

    @Test
    public void deliverWhileRouteResolveException() {
        String tenantId = "tenant1";
        String topic = "testTopic";
        String topicFilter = "testTopic/#";
        String channelId = "channel0";
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId("receiverId")
            .build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).build();
        DeliveryPackage deliveryPack = DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(topicMessagePack).addMatchInfo(matchInfo).build())
            .build();
        DeliveryRequest request = DeliveryRequest.newBuilder().putPackage(tenantId, deliveryPack).build();

        IMQTTTransientSession mockTransientSession = mock(IMQTTTransientSession.class);
        when(mockTransientSession.channelId()).thenReturn(channelId);
        when(mockTransientSession.publish(any(), any())).thenReturn(Collections.emptySet());
        when(localSessionRegistry.get(anyString())).thenReturn(mockTransientSession);

        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(
            Optional.of(CompletableFuture.failedFuture(new RuntimeException("Route resolve exception"))));

        LocalDistService localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);

        CompletableFuture<DeliveryReply> future = localDistService.dist(request);
        DeliveryReply reply = future.join();

        DeliveryResults results = reply.getResultMap().get(tenantId);
        DeliveryResult result = results.getResult(0);
        assertEquals(DeliveryResult.Code.OK, result.getCode());
    }

    @Test
    public void fanOutThrottledDelivery() {
        String tenantId = "tenant1";
        String topic = "testTopic";
        String topicFilter = "testTopic/#";
        String channelId1 = "channel0";
        String channelId2 = "channel1";
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId("receiverId")
            .build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).build();
        DeliveryPackage deliveryPack = DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(topicMessagePack).addMatchInfo(matchInfo).build())
            .build();
        DeliveryRequest request = DeliveryRequest.newBuilder().putPackage(tenantId, deliveryPack).build();

        when(resourceThrottler.hasResource(eq(tenantId), eq(TotalTransientFanOutBytesPerSeconds))).thenReturn(false);

        IMQTTTransientSession mockTransientSession1 = mock(IMQTTTransientSession.class);
        when(mockTransientSession1.channelId()).thenReturn(channelId1);
        when(mockTransientSession1.publish(any(), any())).thenReturn(Collections.emptySet());
        when(localSessionRegistry.get(channelId1)).thenReturn(mockTransientSession1);

        IMQTTTransientSession mockTransientSession2 = mock(IMQTTTransientSession.class);
        when(mockTransientSession2.channelId()).thenReturn(channelId2);
        when(mockTransientSession2.publish(any(), any())).thenReturn(Collections.emptySet());
        when(localSessionRegistry.get(channelId2)).thenReturn(mockTransientSession2);

        ILocalTopicRouter.ILocalRoutes localRoutes = mock(ILocalTopicRouter.ILocalRoutes.class);
        when(localRoutes.localReceiverId()).thenReturn("receiverId");
        when(localRoutes.routesInfo()).thenReturn(new TreeMap<>() {{
            put(channelId1, 1L);
            put(channelId2, 1L);
        }});
        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(
            Optional.of(CompletableFuture.completedFuture(localRoutes)));

        LocalDistService localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);

        CompletableFuture<DeliveryReply> future = localDistService.dist(request);
        DeliveryReply reply = future.join();

        verify(localSessionRegistry).get(eq(channelId1));

        verify(mockTransientSession1).publish(eq(topicMessagePack),
            eq(singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, 1L))));
        verify(mockTransientSession2, never()).publish(any(), any());

        DeliveryResults results = reply.getResultMap().get(tenantId);
        DeliveryResult result = results.getResult(0);
        assertEquals(result.getCode(), DeliveryResult.Code.OK);
    }

    @Test
    public void fanOutThrottledDelivery1() {
        String tenantId = "tenant1";
        String topic1 = "testTopic";
        String topic2 = "testTopic";
        String topicFilter = "testTopic/#";
        String channelId1 = "channel0";
        String channelId2 = "channel1";
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId("receiverId")
            .build();
        DeliveryPackage deliveryPack = DeliveryPackage.newBuilder().addPack(
            DeliveryPack.newBuilder().setMessagePack(TopicMessagePack.newBuilder().setTopic(topic1).build())
                .addMatchInfo(matchInfo).build()).addPack(
            DeliveryPack.newBuilder().setMessagePack(TopicMessagePack.newBuilder().setTopic(topic2).build())
                .addMatchInfo(matchInfo).build()).build();
        DeliveryRequest request = DeliveryRequest.newBuilder().putPackage(tenantId, deliveryPack).build();

        when(resourceThrottler.hasResource(eq(tenantId), eq(TotalTransientFanOutBytesPerSeconds))).thenReturn(false);

        IMQTTTransientSession mockTransientSession1 = mock(IMQTTTransientSession.class);
        when(mockTransientSession1.channelId()).thenReturn(channelId1);
        when(mockTransientSession1.publish(any(), any())).thenReturn(emptySet());
        when(localSessionRegistry.get(channelId1)).thenReturn(mockTransientSession1);

        IMQTTTransientSession mockTransientSession2 = mock(IMQTTTransientSession.class);
        when(mockTransientSession2.channelId()).thenReturn(channelId2);
        when(mockTransientSession2.publish(any(), any())).thenReturn(emptySet());
        when(localSessionRegistry.get(channelId2)).thenReturn(mockTransientSession2);

        ILocalTopicRouter.ILocalRoutes localRoutes = mock(ILocalTopicRouter.ILocalRoutes.class);
        when(localRoutes.localReceiverId()).thenReturn("receiverId");
        when(localRoutes.routesInfo()).thenReturn(new TreeMap<>() {{
            put(channelId1, 1L);
            put(channelId2, 2L);
        }});
        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(
            Optional.of(CompletableFuture.completedFuture(localRoutes)));

        LocalDistService localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);

        CompletableFuture<DeliveryReply> future = localDistService.dist(request);
        DeliveryReply reply = future.join();

        verify(localSessionRegistry, times(2)).get(channelId1);
        verify(mockTransientSession1, times(2)).publish(any(), any());
        verify(mockTransientSession2, never()).publish(any(), any());

        DeliveryResults results = reply.getResultMap().get(tenantId);
        DeliveryResult result = results.getResult(0);
        assertEquals(result.getCode(), DeliveryResult.Code.OK);
    }

    @Test
    public void publishFailedAsNoSub() {
        String tenantId = "tenant1";
        String topic = "testTopic";
        String topicFilter = "testTopic/#";
        String channelId = "channel0";
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setReceiverId("receiverId")
            .build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).build();
        DeliveryPackage deliveryPack = DeliveryPackage.newBuilder()
            .addPack(DeliveryPack.newBuilder().setMessagePack(topicMessagePack).addMatchInfo(matchInfo).build())
            .build();
        DeliveryRequest request = DeliveryRequest.newBuilder().putPackage(tenantId, deliveryPack).build();

        IMQTTTransientSession mockTransientSession = mock(IMQTTTransientSession.class);
        when(mockTransientSession.channelId()).thenReturn(channelId);
        when(mockTransientSession.publish(any(), any())).thenReturn(
            singleton(new IMQTTTransientSession.MatchedTopicFilter(topicFilter, 1L)));
        when(localSessionRegistry.get(anyString())).thenReturn(mockTransientSession);

        ILocalTopicRouter.ILocalRoutes localRoutes = mock(ILocalTopicRouter.ILocalRoutes.class);
        when(localRoutes.localReceiverId()).thenReturn("receiverId");
        when(localRoutes.routesInfo()).thenReturn(Map.of(channelId, 1L));
        when(localTopicRouter.getTopicRoutes(anyString(), any())).thenReturn(
            Optional.of(CompletableFuture.completedFuture(localRoutes)));

        LocalDistService localDistService =
            new LocalDistService(serverId, localSessionRegistry, localTopicRouter, distClient, resourceThrottler);

        CompletableFuture<DeliveryReply> future = localDistService.dist(request);
        DeliveryReply reply = future.join();

        verify(localSessionRegistry).get(anyString());
        verify(mockTransientSession).publish(any(), any());

        DeliveryResults results = reply.getResultMap().get(tenantId);
        DeliveryResult result = results.getResult(0);
        assertEquals(result.getCode(), DeliveryResult.Code.NO_SUB);
    }
}
