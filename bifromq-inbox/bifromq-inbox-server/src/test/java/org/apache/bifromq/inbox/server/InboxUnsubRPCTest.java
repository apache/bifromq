/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.inbox.server;

import static org.apache.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;
import static org.apache.bifromq.inbox.util.InboxServiceUtil.receiverId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.AttachRequest;
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.inbox.rpc.proto.UnsubRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.retain.rpc.proto.MatchReply;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.util.TopicUtil;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class InboxUnsubRPCTest extends InboxServiceTest {
    @Test(groups = "integration")
    public void unsubNoInbox() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        String topicFilter = "/a/b/c";

        UnsubReply unsubReply = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder().build())
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply.getReqId(), reqId);
        assertEquals(unsubReply.getCode(), UnsubReply.Code.NO_INBOX);
        verify(distClient, never()).removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong());
    }

    @Test(groups = "integration")
    public void unsubConflict() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo).setNow(now).build()).join();

        String topicFilter = "/a/b/c";
        UnsubReply unsubReply = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion().toBuilder().setMod(attachReply.getVersion().getMod() + 1).build())
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply.getReqId(), reqId);
        assertEquals(unsubReply.getCode(), UnsubReply.Code.CONFLICT);
        verify(distClient, never()).removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong());
    }

    @Test(groups = "integration")
    public void unsubOK() {
        clearInvocations(distClient);
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong())).thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong())).thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));
        when(retainClient.match(any())).thenReturn(
            CompletableFuture.completedFuture(MatchReply.newBuilder().setResult(MatchReply.Result.OK).build()));

        String topicFilter = "/a/b/c";
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now).build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.OK);

        UnsubReply unsubReply2 = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter(topicFilter)
            .setNow(now).build()).join();
        assertEquals(unsubReply2.getReqId(), reqId);
        assertEquals(unsubReply2.getCode(), UnsubReply.Code.OK);

        CheckReply checkReply = inboxClient.check(CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(getDelivererKey(inboxId, receiverId(inboxId, attachReply.getVersion().getIncarnation())))
            .addMatchInfo(MatchInfo.newBuilder()
                .setReceiverId(receiverId(inboxId, attachReply.getVersion().getIncarnation()))
                .setMatcher(TopicUtil.from(topicFilter))
                .build())
            .build()).join();
        assertEquals(checkReply.getCode(0), CheckReply.Code.NO_SUB);
        verify(distClient, times(1)).removeRoute(anyLong(), eq(tenantId), eq(TopicUtil.from(topicFilter)), anyString(),
            anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void unsubNoSub() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();

        String topicFilter = "/a/b/c";
        UnsubReply unsubReply = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply.getReqId(), reqId);
        assertEquals(unsubReply.getCode(), UnsubReply.Code.NO_SUB);
        verify(distClient, never()).removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong());
    }

    @Test(groups = "integration")
    public void unsubUnderLimit() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
                .setReqId(reqId)
                .setInboxId(inboxId)
                .setExpirySeconds(5)
                .setLimit(10)
                .setDropOldest(true)
                .setLwt(lwt)
                .setClient(clientInfo)
                .setNow(now).build())
            .join();

        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong())).thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong())).thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));
        when(retainClient.match(any())).thenReturn(
            CompletableFuture.completedFuture(MatchReply.newBuilder().setResult(MatchReply.Result.OK).build()));
        SubReply subReply = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter("/a/b/c")
            .setMaxTopicFilters(1)
            .setNow(now)
            .build()).join();
        assertEquals(subReply.getReqId(), reqId);
        assertEquals(subReply.getCode(), SubReply.Code.OK);

        subReply = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter("/a/b")
            .setMaxTopicFilters(1)
            .setNow(now)
            .build()).join();
        assertEquals(subReply.getReqId(), reqId);
        assertEquals(subReply.getCode(), SubReply.Code.EXCEED_LIMIT);

        UnsubReply unsubReply = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter("/a/b/c")
            .setNow(now)
            .build()).join();
        assertEquals(unsubReply.getReqId(), reqId);
        assertEquals(unsubReply.getCode(), UnsubReply.Code.OK);

        subReply = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter("/a/b")
            .setMaxTopicFilters(1)
            .setNow(now)
            .build()).join();
        assertEquals(subReply.getReqId(), reqId);
        assertEquals(subReply.getCode(), SubReply.Code.OK);
    }

    @Test(groups = "integration")
    public void unsubWhenDistUnmatchError() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        AttachReply attachReply = inboxClient.attach(AttachRequest.newBuilder()
            .setReqId(reqId)
            .setInboxId(inboxId)
            .setExpirySeconds(5)
            .setLimit(10)
            .setDropOldest(true)
            .setLwt(lwt)
            .setClient(clientInfo)
            .setNow(now)
            .build()).join();
        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong())).thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        when(retainClient.match(any())).thenReturn(
            CompletableFuture.completedFuture(MatchReply.newBuilder().setResult(MatchReply.Result.OK).build()));

        String topicFilter = "/a/b/c";
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.OK);

        when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong())).thenReturn(CompletableFuture.completedFuture(UnmatchResult.ERROR));
        UnsubReply unsubReply2 = inboxClient.unsub(UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter(topicFilter)
            .setNow(now).build()).join();
        assertEquals(unsubReply2.getReqId(), reqId);
        assertEquals(unsubReply2.getCode(), UnsubReply.Code.ERROR);
    }
}
