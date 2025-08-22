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

import static org.apache.bifromq.inbox.server.Fixtures.matchInfo;
import static org.apache.bifromq.inbox.server.Fixtures.sendRequest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.bifromq.inbox.rpc.proto.SendReply;
import org.apache.bifromq.inbox.rpc.proto.SendRequest;
import org.apache.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import org.apache.bifromq.inbox.storage.proto.InsertRequest;
import org.apache.bifromq.inbox.storage.proto.InsertResult;
import org.apache.bifromq.inbox.storage.proto.MatchedRoute;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.plugin.subbroker.DeliveryResults;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxWriterTest {

    @Mock
    private IInboxInsertScheduler insertScheduler;

    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @SneakyThrows
    @AfterMethod
    public void teardown() {
        closeable.close();
    }

    @Test
    public void insertScheduleErr() {
        when(insertScheduler.schedule(any(InsertRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("err")));
        SendRequest request = sendRequest();
        SendReply sendReply = new InboxWriter(insertScheduler).handle(request).join();
        assertEquals(sendReply, sendReply(request.getReqId(), DeliveryReply.Code.ERROR));
    }

    @Test
    public void insertScheduleNoInbox() {
        when(insertScheduler.schedule(any(InsertRequest.class))).thenReturn(
            CompletableFuture.completedFuture(InsertResult.newBuilder()
                .setCode(InsertResult.Code.NO_INBOX)
                .build()));
        SendRequest request = sendRequest();
        SendReply sendReply = new InboxWriter(insertScheduler).handle(request).join();
        assertEquals(sendReply, sendReply(request.getReqId(), DeliveryResult.Code.NO_RECEIVER));
    }

    @Test
    public void insertScheduleRejected() {
        when(insertScheduler.schedule(any(InsertRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(
                InsertResult.newBuilder()
                    .addResult(InsertResult.SubStatus.newBuilder()
                        .setRejected(true)
                        .setMatchedRoute(MatchedRoute.newBuilder()
                            .setIncarnation(1L)
                            .setTopicFilter("/foo/+")
                            .build())
                        .build())
                    .setCode(InsertResult.Code.OK)
                    .build()));
        SendRequest request = sendRequest();
        SendReply sendReply = new InboxWriter(insertScheduler).handle(request).join();
        assertEquals(sendReply, sendReply(request.getReqId(), DeliveryResult.Code.NO_SUB));
    }

    @Test
    public void insertScheduleOk() {
        when(insertScheduler.schedule(any(InsertRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(InsertResult.newBuilder()
                .setCode(InsertResult.Code.OK)
                .addResult(InsertResult.SubStatus.newBuilder()
                    .setRejected(false)
                    .setMatchedRoute(MatchedRoute.newBuilder()
                        .setTopicFilter("/foo/+")
                        .setIncarnation(1L)
                        .build())
                    .build())
                .build()));
        SendRequest request = sendRequest();
        SendReply sendReply = new InboxWriter(insertScheduler).handle(request).join();
        assertEquals(sendReply, sendReply(request.getReqId(), DeliveryResult.Code.OK));
    }

    private SendReply sendReply(long reqId, DeliveryReply.Code code) {
        return SendReply.newBuilder()
            .setReqId(reqId)
            .setReply(DeliveryReply.newBuilder()
                .setCode(code)
                .build())
            .build();
    }

    private SendReply sendReply(long reqId, DeliveryResult.Code code) {
        return SendReply.newBuilder()
            .setReqId(reqId)
            .setReply(DeliveryReply.newBuilder()
                .setCode(DeliveryReply.Code.OK)
                .putResult("_",
                    DeliveryResults.newBuilder()
                        .addResult(DeliveryResult.newBuilder()
                            .setCode(code)
                            .setMatchInfo(matchInfo())
                            .build())
                        .build())
                .build())
            .build();
    }
}