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

package org.apache.bifromq.dist.server.scheduler;

import org.apache.bifromq.basekv.client.IMutationPipeline;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.client.scheduler.BatchMutationCall;
import org.apache.bifromq.basekv.client.scheduler.MutationCallBatcherKey;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.dist.rpc.proto.BatchMatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchMatchRequest;
import org.apache.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import org.apache.bifromq.dist.rpc.proto.MatchReply;
import org.apache.bifromq.dist.rpc.proto.MatchRequest;
import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.rpc.proto.TenantOption;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BatchMatchCall extends BatchMutationCall<MatchRequest, MatchReply> {
    private final ISettingProvider settingProvider;

    BatchMatchCall(IMutationPipeline pipeline, ISettingProvider settingProvider, MutationCallBatcherKey batcherKey) {
        super(pipeline, batcherKey);
        this.settingProvider = settingProvider;
    }

    @Override
    protected RWCoProcInput makeBatch(Iterable<ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey>> callTasks) {
        BatchMatchRequest.Builder reqBuilder = BatchMatchRequest.newBuilder();
        Map<String, BatchMatchRequest.TenantBatch.Builder> builders = new HashMap<>();
        Iterator<MatchRequest> reqIterator = Iterables.transform(callTasks, ICallTask::call).iterator();
        while (reqIterator.hasNext()) {
            MatchRequest matchReq = reqIterator.next();
            builders.computeIfAbsent(matchReq.getTenantId(), k -> BatchMatchRequest.TenantBatch.newBuilder()
                    .setOption(TenantOption.newBuilder()
                        .setMaxReceiversPerSharedSubGroup(
                            settingProvider.provide(Setting.MaxSharedGroupMembers, matchReq.getTenantId()))
                        .build()))
                .addRoute(MatchRoute.newBuilder()
                    .setMatcher(matchReq.getMatcher())
                    .setReceiverId(matchReq.getReceiverId())
                    .setDelivererKey(matchReq.getDelivererKey())
                    .setBrokerId(matchReq.getBrokerId())
                    .setIncarnation(matchReq.getIncarnation())
                    .build());
        }
        builders.forEach((t, builder) -> reqBuilder.putRequests(t, builder.build()));
        long reqId = System.nanoTime();
        return RWCoProcInput.newBuilder()
            .setDistService(DistServiceRWCoProcInput.newBuilder()
                .setBatchMatch(reqBuilder
                    .setReqId(reqId)
                    .build())
                .build())
            .build();
    }

    @Override
    protected void handleException(ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey> callTask, Throwable e) {
        if (e instanceof ServerNotFoundException) {
            callTask.resultPromise().complete(MatchReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setResult(MatchReply.Result.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof BadVersionException) {
            callTask.resultPromise().complete(MatchReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setResult(MatchReply.Result.TRY_LATER)
                .build());
            return;
        }
        if (e instanceof TryLaterException) {
            callTask.resultPromise().complete(MatchReply.newBuilder()
                .setReqId(callTask.call().getReqId())
                .setResult(MatchReply.Result.TRY_LATER)
                .build());
            return;
        }
        callTask.resultPromise().completeExceptionally(e);
    }

    @Override
    protected void handleOutput(Queue<ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey>> batchedTasks,
                                RWCoProcOutput output) {
        ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey> callTask;
        BatchMatchReply reply = output.getDistService().getBatchMatch();
        Map<String, List<ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey>>> tasksByTenant = new HashMap<>();
        while ((callTask = batchedTasks.poll()) != null) {
            tasksByTenant.computeIfAbsent(callTask.call().getTenantId(), k -> new ArrayList<>()).add(callTask);
        }

        for (String tenantId : tasksByTenant.keySet()) {
            List<ICallTask<MatchRequest, MatchReply, MutationCallBatcherKey>> tasks = tasksByTenant.get(tenantId);
            List<BatchMatchReply.TenantBatch.Code> codes = reply.getResultsMap().get(tenantId).getCodeList();
            assert tasks.size() == codes.size();
            for (int i = 0; i < tasks.size(); i++) {
                MatchReply.Result matchResult = switch (codes.get(i)) {
                    case OK:
                        yield MatchReply.Result.OK;
                    case EXCEED_LIMIT:
                        yield MatchReply.Result.EXCEED_LIMIT;
                    default:
                        log.error("Unexpected match result: {}", codes.get(i));
                        yield MatchReply.Result.ERROR;
                };
                tasks.get(i).resultPromise().complete(MatchReply.newBuilder()
                    .setReqId(tasks.get(i).call().getReqId())
                    .setResult(matchResult)
                    .build());
            }
        }
    }
}
