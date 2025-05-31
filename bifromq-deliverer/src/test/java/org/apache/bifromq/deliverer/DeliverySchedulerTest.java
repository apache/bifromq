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

package org.apache.bifromq.deliverer;

import static org.apache.bifromq.deliverer.DeliveryCallResult.BACK_PRESSURE_REJECTED;
import static org.apache.bifromq.deliverer.DeliveryCallResult.ERROR;
import static org.apache.bifromq.deliverer.DeliveryCallResult.NO_RECEIVER;
import static org.apache.bifromq.deliverer.DeliveryCallResult.NO_SUB;
import static org.apache.bifromq.deliverer.DeliveryCallResult.OK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.plugin.subbroker.DeliveryResults;
import org.apache.bifromq.plugin.subbroker.IDeliverer;
import org.apache.bifromq.plugin.subbroker.ISubBroker;
import org.apache.bifromq.plugin.subbroker.ISubBrokerManager;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.TopicUtil;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeliverySchedulerTest {
    private final String tenantId = "tenant";
    @Mock
    private IDistClient distClient;
    @Mock
    private ISubBrokerManager subBrokerManager;
    @Mock
    private ISubBroker subBroker;
    @Mock
    private IDeliverer groupWriter;
    private IMessageDeliverer testDeliverer;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(subBrokerManager.get(0)).thenReturn(subBroker);
        when(subBroker.open(anyString())).thenReturn(groupWriter);
        when(subBroker.id()).thenReturn(0);
        testDeliverer = new MessageDeliverer(new BatchDeliveryCallBuilderFactory(distClient, subBrokerManager));
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        closeable.close();
    }

    @Test
    public void writeSucceed() {
        MatchInfo matchInfo = MatchInfo.newBuilder().build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1",
                TopicMessagePackHolder.hold(TopicMessagePack.newBuilder().build()));

        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.completedFuture(DeliveryReply.newBuilder()
                .setCode(DeliveryReply.Code.OK)
                .putResult(tenantId, DeliveryResults.newBuilder()
                    .addResult(DeliveryResult.newBuilder()
                        .setMatchInfo(matchInfo)
                        .setCode(DeliveryResult.Code.OK)
                        .build())
                    .build())
                .build()));
        DeliveryCallResult result = testDeliverer.schedule(request).join();
        assertEquals(result, OK);
    }

    @Test
    public void writeIncompleteResult() {
        MatchInfo matchInfo = MatchInfo.newBuilder().build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1",
                TopicMessagePackHolder.hold(TopicMessagePack.newBuilder().build()));
        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.completedFuture(DeliveryReply.newBuilder().build()));
        DeliveryCallResult result = testDeliverer.schedule(request).join();
        assertEquals(result, OK);
    }

    @Test
    public void writeNoSub() {
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from("topic"))
            .setReceiverId("receiverInfo")
            .setIncarnation(1)
            .build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1",
                TopicMessagePackHolder.hold(TopicMessagePack.newBuilder().build()));
        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.completedFuture(DeliveryReply.newBuilder()
                .setCode(DeliveryReply.Code.OK)
                .putResult(tenantId, DeliveryResults.newBuilder()
                    .addResult(DeliveryResult.newBuilder()
                        .setMatchInfo(matchInfo)
                        .setCode(DeliveryResult.Code.NO_SUB)
                        .build())
                    .build())
                .build()));
        DeliveryCallResult result = testDeliverer.schedule(request).join();
        assertEquals(result, NO_SUB);
        verify(distClient).removeRoute(anyLong(),
            eq(tenantId), eq(matchInfo.getMatcher()), eq(matchInfo.getReceiverId()),
            eq("group1"), eq(0), eq(matchInfo.getIncarnation()));
    }

    @Test
    public void writeNoReceiver() {
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from("topic"))
            .setReceiverId("receiverInfo")
            .setIncarnation(1)
            .build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1",
                TopicMessagePackHolder.hold(TopicMessagePack.newBuilder().build()));
        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.completedFuture(DeliveryReply.newBuilder()
                .setCode(DeliveryReply.Code.OK)
                .putResult(tenantId, DeliveryResults.newBuilder()
                    .addResult(DeliveryResult.newBuilder()
                        .setMatchInfo(matchInfo)
                        .setCode(DeliveryResult.Code.NO_RECEIVER)
                        .build())
                    .build())
                .build()));
        DeliveryCallResult result = testDeliverer.schedule(request).join();
        assertEquals(result, NO_RECEIVER);
        verify(distClient).removeRoute(anyLong(),
            eq(tenantId), eq(matchInfo.getMatcher()), eq(matchInfo.getReceiverId()),
            eq("group1"), eq(0), eq(matchInfo.getIncarnation()));
    }

    @Test
    public void writeBackPressureRejected() {
        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from("topic"))
            .setReceiverId("receiverInfo")
            .setIncarnation(1)
            .build();
        DeliveryCall request = new DeliveryCall(tenantId, matchInfo, 0, "group1",
            TopicMessagePackHolder.hold(TopicMessagePack.newBuilder().build()));
        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.completedFuture(DeliveryReply.newBuilder()
                .setCode(DeliveryReply.Code.BACK_PRESSURE_REJECTED)
                .build()));
        DeliveryCallResult result = testDeliverer.schedule(request).join();
        assertEquals(result, BACK_PRESSURE_REJECTED);
    }

    @Test
    public void writeFail() {
        MatchInfo matchInfo = MatchInfo.newBuilder().build();
        DeliveryCall request =
            new DeliveryCall(tenantId, matchInfo, 0, "group1",
                TopicMessagePackHolder.hold(TopicMessagePack.newBuilder().build()));
        when(groupWriter.deliver(any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mock Exception")));
        DeliveryCallResult result = testDeliverer.schedule(request).join();
        assertEquals(result, ERROR);
    }
}
