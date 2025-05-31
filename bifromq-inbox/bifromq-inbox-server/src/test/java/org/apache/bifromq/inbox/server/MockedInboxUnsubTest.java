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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basescheduler.exception.BatcherUnavailableException;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.inbox.rpc.proto.UnsubRequest;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class MockedInboxUnsubTest extends MockedInboxService {
    @Test
    public void unsubNoInbox() {
        long reqId = HLC.INST.getPhysical();
        when(unsubScheduler.schedule(any()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder()
                .setReqId(reqId)
                .setCode(UnsubReply.Code.NO_INBOX)
                .build()));

        StreamObserver<UnsubReply> streamObserver = mock(StreamObserver.class);
        inboxService.unsub(UnsubRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == UnsubReply.Code.NO_INBOX));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void noSub() {
        long reqId = HLC.INST.getPhysical();
        when(unsubScheduler.schedule(any()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder()
                .setReqId(reqId)
                .setCode(UnsubReply.Code.NO_SUB)
                .build()));

        StreamObserver<UnsubReply> streamObserver = mock(StreamObserver.class);
        inboxService.unsub(UnsubRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == UnsubReply.Code.NO_SUB));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void unsubError() {
        long reqId = HLC.INST.getPhysical();
        when(unsubScheduler.schedule(any()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder()
                .setReqId(reqId)
                .setCode(UnsubReply.Code.ERROR)
                .build()));

        StreamObserver<UnsubReply> streamObserver = mock(StreamObserver.class);
        inboxService.unsub(UnsubRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == UnsubReply.Code.ERROR));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void unsubOk() {
        long reqId = HLC.INST.getPhysical();
        when(unsubScheduler.schedule(any()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder()
                .setReqId(reqId)
                .setCode(UnsubReply.Code.OK)
                .build()));
        when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.OK));

        StreamObserver<UnsubReply> streamObserver = mock(StreamObserver.class);
        inboxService.unsub(UnsubRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == UnsubReply.Code.OK));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void unmatchError() {
        long reqId = HLC.INST.getPhysical();
        when(unsubScheduler.schedule(any()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder()
                .setReqId(reqId)
                .setCode(UnsubReply.Code.OK)
                .build()));
        when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(UnmatchResult.ERROR));

        StreamObserver<UnsubReply> streamObserver = mock(StreamObserver.class);
        inboxService.unsub(UnsubRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == UnsubReply.Code.ERROR));
        verify(streamObserver).onCompleted();
    }

    @Test
    public void unsubThrowsException() {
        long reqId = HLC.INST.getPhysical();
        when(unsubScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));

        StreamObserver<UnsubReply> streamObserver = mock(StreamObserver.class);
        inboxService.unsub(UnsubRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == UnsubReply.Code.TRY_LATER));
        verify(streamObserver).onCompleted();
    }
}
