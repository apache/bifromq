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

package org.apache.bifromq.dist.server.handler;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.dist.rpc.proto.UnmatchReply;
import org.apache.bifromq.dist.rpc.proto.UnmatchRequest;
import org.apache.bifromq.dist.server.scheduler.IUnmatchCallScheduler;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.distservice.UnmatchError;
import org.apache.bifromq.plugin.eventcollector.distservice.Unmatched;
import org.apache.bifromq.util.TopicUtil;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UnmatchReqHandlerTest {
    private UnmatchReqHandler handler;
    private IEventCollector eventCollector;
    private IUnmatchCallScheduler unmatchCallScheduler;

    @BeforeMethod
    public void setUp() {
        eventCollector = mock(IEventCollector.class);
        unmatchCallScheduler = mock(IUnmatchCallScheduler.class);
        handler = new UnmatchReqHandler(eventCollector, unmatchCallScheduler);
    }

    @Test
    public void testHandleSuccess() throws ExecutionException, InterruptedException {
        UnmatchRequest request = UnmatchRequest.newBuilder()
            .setReqId(1)
            .setTenantId("tenant123")
            .setReceiverId("receiver456")
            .setMatcher(TopicUtil.from("topic"))
            .setBrokerId(10)
            .setDelivererKey("key123")
            .setIncarnation(1L)
            .build();
        UnmatchReply reply = UnmatchReply.newBuilder()
            .setReqId(1)
            .setResult(UnmatchReply.Result.OK)
            .build();
        CompletableFuture<UnmatchReply> future = CompletableFuture.completedFuture(reply);
        when(unmatchCallScheduler.schedule(request)).thenReturn(future);

        CompletableFuture<UnmatchReply> result = handler.handle(request);
        assertNotNull(result);
        assertEquals(result.get().getResult(), UnmatchReply.Result.OK);
        verify(eventCollector, times(1)).report(any(Unmatched.class));
    }

    @Test
    public void testHandleError() throws ExecutionException, InterruptedException {
        UnmatchRequest request = UnmatchRequest.newBuilder()
            .setReqId(3)
            .setTenantId("tenant456")
            .setReceiverId("receiver789")
            .setMatcher(TopicUtil.from("errorTopic"))
            .setBrokerId(20)
            .setDelivererKey("keyError")
            .setIncarnation(1L)
            .build();
        UnmatchReply reply = UnmatchReply.newBuilder()
            .setReqId(3)
            .setResult(UnmatchReply.Result.ERROR)
            .build();
        CompletableFuture<UnmatchReply> future = CompletableFuture.completedFuture(reply);
        when(unmatchCallScheduler.schedule(request)).thenReturn(future);

        CompletableFuture<UnmatchReply> result = handler.handle(request);
        assertNotNull(result);
        assertEquals(result.get().getResult(), UnmatchReply.Result.ERROR);
        verify(eventCollector, times(1)).report(argThat(e ->
            e.type() == EventType.UNMATCH_ERROR && ((UnmatchError) e).reason().contains("Internal Error")));
    }

    @Test
    public void testHandleBackPressureException() {
        UnmatchRequest request = UnmatchRequest.newBuilder().setReqId(2).build();
        CompletableFuture<UnmatchReply> failedFuture = CompletableFuture.supplyAsync(() -> {
            throw new BackPressureException("Back pressure");
        });
        when(unmatchCallScheduler.schedule(request)).thenReturn(failedFuture);

        UnmatchReply result = handler.handle(request).join();
        assertEquals(result.getResult(), UnmatchReply.Result.BACK_PRESSURE_REJECTED);
        verify(eventCollector, times(1)).report(argThat(e ->
            e.type() == EventType.UNMATCH_ERROR && ((UnmatchError) e).reason().contains("Back pressure")));
    }

    @Test
    public void testCloseMethod() {
        handler.close();
        verify(unmatchCallScheduler, times(1)).close();
    }
}
