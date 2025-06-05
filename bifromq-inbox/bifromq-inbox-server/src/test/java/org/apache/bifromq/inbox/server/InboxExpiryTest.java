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

package org.apache.bifromq.inbox.server;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.AttachRequest;
import org.apache.bifromq.inbox.rpc.proto.DetachRequest;
import org.apache.bifromq.inbox.rpc.proto.ExistReply;
import org.apache.bifromq.inbox.rpc.proto.ExistRequest;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.util.TopicUtil;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class InboxExpiryTest extends InboxServiceTest {
    @Test(groups = "integration")
    public void lwtRetained() {
        clearInvocations(retainClient, eventCollector);
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder()
            .setTopic("LastWill")
            .setMessage(Message.newBuilder()
                .setIsRetain(true)
                .setPayload(ByteString.copyFromUtf8("last will"))
                .build())
            .setDelaySeconds(1)
            .build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(distClient.pub(anyLong(), anyString(), any(), any())).thenReturn(
            CompletableFuture.completedFuture(PubResult.OK));
        when(retainClient.retain(anyLong(), anyString(), any(), any(), anyInt(), any())).thenReturn(
            CompletableFuture.completedFuture(
                RetainReply.newBuilder().setResult(RetainReply.Result.RETAINED).build()));
        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(2)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setExpirySeconds(1)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        verify(retainClient, timeout(10000).times(1)).retain(anyLong(), anyString(), any(), any(), anyInt(), any());
        ArgumentCaptor<Event<?>> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventCollector, timeout(10000).times(4)).report(eventCaptor.capture());
        assertEquals(eventCaptor.getAllValues().get(0).type(), EventType.MQTT_SESSION_START);
        assertTrue(eventCaptor.getAllValues()
            .stream().map(Event::type)
            .collect(Collectors.toSet())
            .containsAll(List.of(EventType.MSG_RETAINED, EventType.WILL_DISTED, EventType.MQTT_SESSION_STOP)));
        await().until(() -> {
            ExistReply existReply = inboxClient.exist(ExistRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(0)
                .build()).join();
            return existReply.getCode() == ExistReply.Code.NO_INBOX;
        });
    }

    @Test(groups = "integration")
    public void lwtRetryOnError() {
        clearInvocations(distClient, retainClient, eventCollector);
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill")
            .setMessage(Message.newBuilder().setIsRetain(true).build())
            .setDelaySeconds(1)
            .build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(PubResult.OK));
        when(retainClient.retain(anyLong(), anyString(), any(), any(), anyInt(), any())).thenReturn(
            CompletableFuture.completedFuture(
                RetainReply.newBuilder().setResult(RetainReply.Result.TRY_LATER).build()),
            CompletableFuture.completedFuture(
                RetainReply.newBuilder().setResult(RetainReply.Result.RETAINED).build()));
        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(2)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setExpirySeconds(10)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        verify(distClient, timeout(10000).times(2)).pub(anyLong(), anyString(), any(), any());
        verify(retainClient, timeout(10000).times(2)).retain(anyLong(), anyString(), any(), any(), anyInt(), any());
        await().until(() -> {
            ExistReply existReply = inboxClient.exist(ExistRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(0)
                .build()).join();
            return existReply.getCode() == ExistReply.Code.NO_INBOX;
        });
    }

    @Test(groups = "integration")
    public void lwtAfterDetach() {
        clearInvocations(distClient);
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill")
            .setDelaySeconds(1)
            .setMessage(Message.newBuilder()
                .setPubQoS(QoS.AT_LEAST_ONCE)
                .setPayload(ByteString.copyFromUtf8("last will"))
                .build())
            .build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(PubResult.OK));
        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(10)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setExpirySeconds(10)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        verify(distClient, timeout(2000).times(1))
            .pub(anyLong(), eq(lwt.getTopic()),
                argThat(m -> m.getPubQoS() == QoS.AT_LEAST_ONCE
                    && m.getPayload().equals(lwt.getMessage().getPayload())),
                any());
        verify(eventCollector, timeout(2000)).report(argThat(e -> e.type() == EventType.WILL_DISTED));
    }

    @Test(groups = "integration")
    public void matchCleanupWhenInboxExpired() {
        clearInvocations(distClient, retainClient, eventCollector);
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();
        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(2)
            .setLimit(10)
            .setDropOldest(true)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        String topicFilter = "/a/b/c";
        inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder()
                .setIncarnation(1L)
                .build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build()).join();
        inboxClient.detach(DetachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setExpirySeconds(1)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        verify(distClient, timeout(10000).times(0)).pub(anyLong(), anyString(), any(), any());
        verify(distClient, timeout(10000).times(1))
            .removeRoute(anyLong(), eq(tenantId), eq(TopicUtil.from(topicFilter)), anyString(), anyString(), anyInt(),
                eq(1L));
        await().until(() -> {
            ExistReply existReply = inboxClient.exist(ExistRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(0)
                .build()).join();
            return existReply.getCode() == ExistReply.Code.NO_INBOX;
        });
    }
}
