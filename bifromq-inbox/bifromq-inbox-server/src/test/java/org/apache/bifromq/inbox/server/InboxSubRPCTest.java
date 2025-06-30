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

import static org.apache.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;
import static org.apache.bifromq.inbox.util.InboxServiceUtil.receiverId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.AttachRequest;
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.Test;

public class InboxSubRPCTest extends InboxServiceTest {
    @Test(groups = "integration")
    public void subNoInbox() {
        clearInvocations(distClient);
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        String topicFilter = "/a/b/c";

        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder().build())
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder()
                .setIncarnation(1L)
                .build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.NO_INBOX);

        CheckReply checkReply = inboxClient.check(CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(getDelivererKey(inboxId, receiverId(inboxId, 0)))
            .addMatchInfo(MatchInfo.newBuilder()
                .setReceiverId(receiverId(inboxId, 0))
                .setMatcher(TopicUtil.from(topicFilter))
                .build())
            .build()).join();
        assertEquals(checkReply.getCode(0), CheckReply.Code.NO_RECEIVER);

        verify(distClient, times(0))
            .addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), eq(1L));

    }

    @Test(groups = "integration")
    public void subConflict() {
        long now = System.nanoTime();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        LWT lwt = LWT.newBuilder().setTopic("LastWill").setDelaySeconds(5).build();
        ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId(tenantId).build();

        AttachReply reply = inboxClient.attach(AttachRequest.newBuilder()
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
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(reply.getVersion().toBuilder().setMod(reply.getVersion().getMod() + 1).build())
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.CONFLICT);

        verify(distClient, times(0)).addRoute(anyLong(), anyString(), any(), anyString(), anyString(),
            anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void subOK() {
        clearInvocations(distClient);
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        String tenantId = "traffic-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        String topicFilter = "/a/b/c";
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

        CheckReply checkReply = inboxClient.check(CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(getDelivererKey(inboxId, receiverId(inboxId, attachReply.getVersion().getIncarnation())))
            .addMatchInfo(MatchInfo.newBuilder()
                .setReceiverId(receiverId(inboxId, attachReply.getVersion().getIncarnation()))
                .setMatcher(TopicUtil.from(topicFilter))
                .build())
            .build()).join();
        assertEquals(checkReply.getCode(0), CheckReply.Code.NO_SUB);

        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.OK);

        checkReply = inboxClient.check(CheckRequest.newBuilder()
            .setTenantId(tenantId)
            .setDelivererKey(getDelivererKey(inboxId, receiverId(inboxId, attachReply.getVersion().getIncarnation())))
            .addMatchInfo(MatchInfo.newBuilder()
                .setReceiverId(receiverId(inboxId, attachReply.getVersion().getIncarnation()))
                .setMatcher(TopicUtil.from(topicFilter))
                .build())
            .build()).join();
        assertEquals(checkReply.getCode(0), CheckReply.Code.OK);
        verify(distClient, times(1))
            .addRoute(anyLong(), eq(tenantId), eq(TopicUtil.from(topicFilter)), anyString(), anyString(), anyInt(),
                anyLong());
    }

    @Test(groups = "integration")
    public void subExists() {
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

        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.EXISTS);

        verify(distClient, times(2))
            .addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void subExceedLimit() {
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

        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter("/a/b/c")
            .setMaxTopicFilters(1)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.OK);

        subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter("/a/b")
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.EXCEED_LIMIT);

        verify(distClient, times(1))
            .addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong());
    }

    @Test(groups = "integration")
    public void subErrorWhenExceedDistMatchLimit() {
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

        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.EXCEED_LIMIT));

        String topicFilter = "/a/b/c";
        SubReply subReply2 = inboxClient.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(attachReply.getVersion())
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build()).join();
        assertEquals(subReply2.getReqId(), reqId);
        assertEquals(subReply2.getCode(), SubReply.Code.EXCEED_LIMIT);
    }

    @Test(groups = "integration")
    public void subErrorWhenDistMatchError() {
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

        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.ERROR));

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
        assertEquals(subReply2.getCode(), SubReply.Code.ERROR);
    }
}
