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

import static org.apache.bifromq.inbox.util.InboxServiceUtil.receiverId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basescheduler.exception.BatcherUnavailableException;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.RetainHandling;
import org.apache.bifromq.inbox.storage.proto.TopicFilterOption;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.retain.rpc.proto.MatchReply;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.util.TopicUtil;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class MockedInboxSubTest extends MockedInboxService {
    @Test
    public void subOK() {
        sub(SubReply.Code.OK);
    }

    @Test
    public void subExists() {
        sub(SubReply.Code.EXISTS);
    }

    @Test
    public void retainDisabled() {
        long reqId = HLC.INST.getPhysical();
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant";
        String inboxId = "inbox";
        String topicFilter = "/a/b/c";
        long incarnation = 1;
        long version = 1;
        when(settingProvider.provide(Setting.RetainEnabled, tenantId)).thenReturn(false);
        when(subScheduler.schedule(any())).thenReturn(CompletableFuture
            .completedFuture(SubReply.newBuilder()
                .setReqId(reqId)
                .setCode(SubReply.Code.OK)
                .build()));
        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));

        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build())
            .setVersion(InboxVersion.newBuilder().setMod(version).setIncarnation(incarnation).build())
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build(), streamObserver);

        verify(retainClient, never()).match(any());
    }

    @Test
    public void sendAtSubscribe() {
        sendAtSubscribe(true);
        sendAtSubscribe(false);
    }

    private void sendAtSubscribe(boolean exist) {
        long reqId = HLC.INST.getPhysical();
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant";
        String inboxId = "inbox";
        String topicFilter = "/a/b/c";
        QoS qos = QoS.AT_LEAST_ONCE;
        long incarnation = 1;
        long version = 1;
        when(subScheduler.schedule(any())).thenReturn(CompletableFuture
            .completedFuture(SubReply.newBuilder()
                .setReqId(reqId)
                .setCode(exist ? SubReply.Code.EXISTS : SubReply.Code.OK)
                .build()));
        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setOption(TopicFilterOption.newBuilder()
                .setQos(qos)
                .setRetainHandling(RetainHandling.SEND_AT_SUBSCRIBE)
                .build())
            .setVersion(InboxVersion.newBuilder().setMod(version).setIncarnation(incarnation).build())
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build(), streamObserver);
    }

    @Test
    public void sendAtSubscribeIf() {
        sendAtSubscribeIf(true);
        sendAtSubscribeIf(false);
    }

    private void sendAtSubscribeIf(boolean exists) {
        long reqId = HLC.INST.getPhysical();
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant";
        String inboxId = "inbox";
        String topicFilter = "/a/b/c";
        QoS qos = QoS.AT_LEAST_ONCE;
        long incarnation = 1;
        long version = 1;
        when(subScheduler.schedule(any())).thenReturn(CompletableFuture
            .completedFuture(SubReply.newBuilder()
                .setReqId(reqId)
                .setCode(exists ? SubReply.Code.EXISTS : SubReply.Code.OK)
                .build()));
        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));
        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setOption(TopicFilterOption.newBuilder()
                .setQos(qos)
                .setRetainHandling(RetainHandling.SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS)
                .build())
            .setVersion(InboxVersion.newBuilder().setMod(version).setIncarnation(incarnation).build())
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build(), streamObserver);
    }

    @Test
    public void sharedSubNoRetain() {
        sharedSubNoRetain("$share/group1/a/b/c");
        sharedSubNoRetain("$oshare/group1/a/b/c");
    }

    private void sharedSubNoRetain(String sharedSubTopicFilter) {
        long reqId = HLC.INST.getPhysical();
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant";
        String inboxId = "inbox";
        long incarnation = 1;
        long version = 1;
        when(subScheduler.schedule(any())).thenReturn(CompletableFuture
            .completedFuture(SubReply.newBuilder()
                .setReqId(reqId)
                .setCode(SubReply.Code.OK)
                .build()));
        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));

        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build())
            .setVersion(InboxVersion.newBuilder().setMod(version).setIncarnation(incarnation).build())
            .setTopicFilter(sharedSubTopicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build(), streamObserver);

        verify(retainClient, never()).match(any());
    }

    @Test
    public void subExceedLimit() {
        long reqId = HLC.INST.getPhysical();
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant";
        String inboxId = "inbox";
        long incarnation = 1;
        long version = 1;
        String topicFilter = "/a/b/c";
        when(subScheduler.schedule(any())).thenReturn(
            CompletableFuture.completedFuture(SubReply.newBuilder()
                .setReqId(reqId)
                .setCode(SubReply.Code.OK)
                .build()));

        when(retainClient.match(any())).thenReturn(CompletableFuture.completedFuture(MatchReply.newBuilder()
            .setResult(MatchReply.Result.OK).build()));

        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.EXCEED_LIMIT));

        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build())
            .setVersion(InboxVersion.newBuilder().setMod(version).setIncarnation(incarnation).build())
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build(), streamObserver);

        verify(distClient).addRoute(eq(reqId), eq(tenantId), eq(TopicUtil.from(topicFilter)),
            eq(receiverId(inboxId, incarnation)), anyString(), eq(1), anyLong());
        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == SubReply.Code.EXCEED_LIMIT));
        verify(streamObserver).onCompleted();
    }

    private void sub(SubReply.Code code) {
        long reqId = HLC.INST.getPhysical();
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant";
        String inboxId = "inbox";
        long incarnation = 1;
        long version = 1;
        String topicFilter = "/a/b/c";
        when(subScheduler.schedule(any())).thenReturn(
            CompletableFuture.completedFuture(SubReply.newBuilder()
                .setReqId(reqId)
                .setCode(code)
                .build()));
        when(retainClient.match(any())).thenReturn(CompletableFuture.completedFuture(MatchReply.newBuilder()
            .setResult(MatchReply.Result.OK).build()));
        when(distClient.addRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(MatchResult.OK));

        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.AT_LEAST_ONCE).build())
            .setVersion(InboxVersion.newBuilder().setMod(version).setIncarnation(incarnation).build())
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build(), streamObserver);

        verify(distClient).addRoute(eq(reqId), eq(tenantId), eq(TopicUtil.from(topicFilter)),
            eq(receiverId(inboxId, incarnation)), anyString(), eq(1), anyLong());
        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == code));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void subThrowsException() {
        long reqId = HLC.INST.getPhysical();
        when(subScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));

        StreamObserver<SubReply> streamObserver = mock(StreamObserver.class);
        inboxService.sub(SubRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == SubReply.Code.TRY_LATER));
        verify(streamObserver).onCompleted();
    }
}
