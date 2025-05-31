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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basescheduler.exception.BatcherUnavailableException;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;

public class MockedInboxCommitTest extends MockedInboxService {
    @Test
    public void commitThrowsException() {
        long reqId = HLC.INST.getPhysical();
        when(commitScheduler.schedule(any())).thenReturn(
            CompletableFuture.failedFuture(new BatcherUnavailableException("Mocked")));

        StreamObserver<CommitReply> streamObserver = mock(StreamObserver.class);
        inboxService.commit(CommitRequest.newBuilder().setReqId(reqId).build(), streamObserver);

        verify(streamObserver).onNext(argThat(reply ->
            reply.getReqId() == reqId && reply.getCode() == CommitReply.Code.TRY_LATER));
        verify(streamObserver).onCompleted();
    }
}
