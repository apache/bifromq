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

package org.apache.bifromq.dist.server.scheduler;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.client.IMutationPipeline;
import org.apache.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.dist.rpc.proto.BatchMatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchMatchRequest;
import org.apache.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import org.apache.bifromq.dist.rpc.proto.MatchReply;
import org.apache.bifromq.dist.rpc.proto.MatchRequest;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.util.TopicUtil;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchMatchCallTest {

    private KVRangeId rangeId;
    private IMutationPipeline pipeline;
    private ISettingProvider settingProvider;
    private BatchMatchCall batchMatchCall;

    @BeforeMethod
    void setUp() {
        rangeId = KVRangeId.newBuilder().setId(1).build();
        pipeline = mock(IMutationPipeline.class);
        settingProvider = mock(ISettingProvider.class);
        batchMatchCall = new BatchMatchCall(pipeline, settingProvider,
            new MutationCallBatcherKey(rangeId, "leaderStoreId", 1L));
    }

    @Test
    void testMakeBatch() {
        MatchRequest request1 = MatchRequest.newBuilder()
            .setReqId(1)
            .setTenantId("tenant1")
            .setMatcher(TopicUtil.from("filter1"))
            .setReceiverId("receiver1")
            .setBrokerId(1)
            .setDelivererKey("key1")
            .setIncarnation(1L)
            .build();

        MatchRequest request2 = MatchRequest.newBuilder()
            .setReqId(2)
            .setTenantId("tenant2")
            .setMatcher(TopicUtil.from("filter2"))
            .setReceiverId("receiver2")
            .setBrokerId(2)
            .setDelivererKey("key2")
            .setIncarnation(1L)
            .build();

        class CallTask implements ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey> {
            final MatchRequest request;

            CallTask(MatchRequest request) {
                this.request = request;
            }

            @Override
            public MatchRequest call() {
                return request;
            }

            @Override
            public CompletableFuture<MatchReply> resultPromise() {
                return null;
            }

            @Override
            public MutationCallBatcherKey batcherKey() {
                return null;
            }

            @Override
            public long ts() {
                return 0;
            }
        }

        // contain duplicate request
        Iterable<ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey>> callTasks =
            List.of(new CallTask(request1), new CallTask(request1), new CallTask(request2));

        when(settingProvider.provide(Setting.MaxSharedGroupMembers, "tenant1")).thenReturn(100);
        when(settingProvider.provide(Setting.MaxSharedGroupMembers, "tenant2")).thenReturn(200);

        RWCoProcInput input = batchMatchCall.makeBatch(callTasks);

        BatchMatchRequest batchRequest = input.getDistService().getBatchMatch();
        assertEquals(batchRequest.getRequestsCount(), 2);

        Map<String, BatchMatchRequest.TenantBatch> options = batchRequest.getRequestsMap();
        assertEquals(options.get("tenant1").getOption().getMaxReceiversPerSharedSubGroup(), 100);
        assertEquals(options.get("tenant2").getOption().getMaxReceiversPerSharedSubGroup(), 200);
    }

    private void testHandleOutput(BatchMatchReply.TenantBatch.Code batchResult, MatchReply.Result expectedMatchResult) {
        ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey> callTask = mock(ICallTask.class);
        MatchRequest request = MatchRequest.newBuilder()
            .setReqId(1)
            .setTenantId("tenant1")
            .setMatcher(TopicUtil.from("filter1"))
            .setReceiverId("receiver1")
            .setBrokerId(1)
            .setDelivererKey("key1")
            .setIncarnation(1L)
            .build();
        when(callTask.call()).thenReturn(request);
        CompletableFuture<MatchReply> resultPromise = new CompletableFuture<>();
        when(callTask.resultPromise()).thenReturn(resultPromise);

        when(settingProvider.provide(Setting.MaxSharedGroupMembers, "tenant1")).thenReturn(100);

        Queue<ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey>> batchedTasks = new LinkedList<>();
        batchedTasks.add(callTask);

        BatchMatchReply batchMatchReply = BatchMatchReply.newBuilder().setReqId(1)
            .putResults(request.getTenantId(), BatchMatchReply.TenantBatch.newBuilder().addCode(batchResult).build())
            .build();
        RWCoProcOutput output = RWCoProcOutput.newBuilder()
            .setDistService(DistServiceRWCoProcOutput.newBuilder().setBatchMatch(batchMatchReply).build()).build();

        batchMatchCall.handleOutput(batchedTasks, output);

        verify(callTask).resultPromise();
        MatchReply reply = resultPromise.join();
        assertEquals(expectedMatchResult, reply.getResult());
        assertEquals(reply.getReqId(), 1);
    }

    @Test
    void testHandleOutput() {
        testHandleOutput(BatchMatchReply.TenantBatch.Code.OK, MatchReply.Result.OK);
        testHandleOutput(BatchMatchReply.TenantBatch.Code.EXCEED_LIMIT, MatchReply.Result.EXCEED_LIMIT);
    }

    @Test
    void testHandleException() {
        ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey> callTask = mock(ICallTask.class);

        Throwable exception = new RuntimeException("Test exception");
        CompletableFuture<MatchReply> resultPromise = new CompletableFuture<>();
        when(callTask.resultPromise()).thenReturn(resultPromise);

        batchMatchCall.handleException(callTask, exception);

        verify(callTask).resultPromise();
        assertTrue(resultPromise.isCompletedExceptionally());
    }
}
