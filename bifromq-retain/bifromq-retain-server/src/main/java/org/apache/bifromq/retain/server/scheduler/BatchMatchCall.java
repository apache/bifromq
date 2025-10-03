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

package org.apache.bifromq.retain.server.scheduler;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;
import static org.apache.bifromq.retain.rpc.proto.MatchReply.Result.OK;
import static org.apache.bifromq.retain.server.scheduler.BatchMatchCallHelper.parallelMatch;
import static org.apache.bifromq.retain.server.scheduler.BatchMatchCallHelper.serialMatch;
import static org.apache.bifromq.retain.server.scheduler.MatchCallRangeRouter.rangeLookup;
import static org.apache.bifromq.util.TopicUtil.isWildcardTopicFilter;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.client.exception.BadRequestException;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.InternalErrorException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.ICallTask;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.retain.rpc.proto.BatchMatchReply;
import org.apache.bifromq.retain.rpc.proto.BatchMatchRequest;
import org.apache.bifromq.retain.rpc.proto.MatchParam;
import org.apache.bifromq.retain.rpc.proto.MatchReply;
import org.apache.bifromq.retain.rpc.proto.MatchResult;
import org.apache.bifromq.retain.rpc.proto.RetainServiceROCoProcInput;

@Slf4j
class BatchMatchCall implements IBatchCall<MatchRetainedRequest, MatchRetainedResult, MatchCallBatcherKey> {
    private final MatchCallBatcherKey batcherKey;
    private final IBaseKVStoreClient retainStoreClient;
    private final ISettingProvider settingProvider;
    private Queue<ICallTask<MatchRetainedRequest, MatchRetainedResult, MatchCallBatcherKey>> tasks =
        new ArrayDeque<>(128);
    private Set<String> nonWildcardTopicFilters = new HashSet<>(128);
    private Set<String> wildcardTopicFilters = new HashSet<>(128);

    BatchMatchCall(MatchCallBatcherKey batcherKey, IBaseKVStoreClient retainStoreClient,
                   ISettingProvider settingProvider) {
        this.batcherKey = batcherKey;
        this.retainStoreClient = retainStoreClient;
        this.settingProvider = settingProvider;
    }

    @Override
    public void add(ICallTask<MatchRetainedRequest, MatchRetainedResult, MatchCallBatcherKey> task) {
        tasks.add(task);
        if (isWildcardTopicFilter(task.call().topicFilter())) {
            wildcardTopicFilters.add(task.call().topicFilter());
        } else {
            nonWildcardTopicFilters.add(task.call().topicFilter());
        }
    }

    @Override
    public void reset(boolean abort) {
        if (abort) {
            tasks = new ArrayDeque<>(128);
            nonWildcardTopicFilters = new HashSet<>(128);
            wildcardTopicFilters = new HashSet<>(128);
        } else {
            nonWildcardTopicFilters.clear();
            wildcardTopicFilters.clear();
        }
    }

    @Override
    public CompletableFuture<Void> execute() {
        return execute(tasks, nonWildcardTopicFilters, wildcardTopicFilters);
    }

    private CompletableFuture<Void> execute(
        Queue<ICallTask<MatchRetainedRequest, MatchRetainedResult, MatchCallBatcherKey>> tasks,
        Set<String> nonWildcardTopicFilters,
        Set<String> wildcardTopicFilters) {
        long now = HLC.INST.getPhysical();
        long reqId = System.nanoTime();
        NavigableMap<Boundary, KVRangeSetting> effectiveRouter = retainStoreClient.latestEffectiveRouter();

        Map<KVRangeSetting, Set<String>> parallelMatches =
            rangeLookup(batcherKey.tenantId(), nonWildcardTopicFilters, effectiveRouter);
        CompletableFuture<Map<String, MatchResult>> parallelMatchFutures =
            parallelMatch(reqId, now, parallelMatches, this::match);
        CompletableFuture<Map<String, MatchResult>> wildcardMatchFuture;
        if (wildcardTopicFilters.isEmpty()) {
            wildcardMatchFuture = CompletableFuture.completedFuture(Collections.emptyMap());
        } else {
            int limit = settingProvider.provide(Setting.RetainMessageMatchLimit, batcherKey.tenantId());
            Map<KVRangeSetting, Set<String>> serialMatches =
                rangeLookup(batcherKey.tenantId(), wildcardTopicFilters, effectiveRouter);
            wildcardMatchFuture = serialMatch(reqId, now, serialMatches, limit, this::match);
        }

        return CompletableFuture.allOf(parallelMatchFutures, wildcardMatchFuture)
            .handle(unwrap((v, e) -> {
                ICallTask<MatchRetainedRequest, MatchRetainedResult, MatchCallBatcherKey> task;
                if (e != null) {
                    if (e instanceof ServerNotFoundException
                        || e instanceof TryLaterException
                        || e instanceof BadVersionException) {
                        while ((task = tasks.poll()) != null) {
                            task.resultPromise()
                                .complete(
                                    new MatchRetainedResult(MatchReply.Result.TRY_LATER, Collections.emptyList()));
                        }
                        return null;
                    }
                    while ((task = tasks.poll()) != null) {
                        task.resultPromise()
                            .complete(new MatchRetainedResult(MatchReply.Result.ERROR, Collections.emptyList()));
                    }
                } else {
                    // aggregate result from each reply
                    Map<String, MatchResult> aggregatedResults = new HashMap<>();
                    aggregatedResults.putAll(parallelMatchFutures.join());
                    aggregatedResults.putAll(wildcardMatchFuture.join());
                    while ((task = tasks.poll()) != null) {
                        MatchResult matchResult = aggregatedResults.get(task.call().topicFilter());
                        task.resultPromise().complete(new MatchRetainedResult(OK, matchResult.getMessagesList()));
                    }
                }
                return null;
            }));
    }

    private CompletableFuture<Map<String, MatchResult>> match(long reqId, long now, Map<String, Integer> topicFilters,
                                                              KVRangeSetting rangeSetting) {
        BatchMatchRequest request = BatchMatchRequest.newBuilder().putMatchParams(batcherKey.tenantId(),
            MatchParam.newBuilder().putAllTopicFilters(topicFilters).setNow(now).build()).setReqId(reqId).build();
        return queryCoProc(request, rangeSetting)
            .thenApply(reply -> reply.getResultPackMap()
                .get(batcherKey.tenantId())
                .getResultsMap());
    }

    private CompletableFuture<BatchMatchReply> queryCoProc(BatchMatchRequest request, KVRangeSetting rangeSetting) {
        Optional<String> replica = rangeSetting.randomReplicaForQuery();
        if (replica.isEmpty()) {
            return CompletableFuture.failedFuture(new TryLaterException());
        }
        return retainStoreClient.query(replica.get(),
                KVRangeRORequest.newBuilder().setReqId(request.getReqId()).setKvRangeId(rangeSetting.id())
                    .setVer(rangeSetting.ver()).setRoCoProc(ROCoProcInput.newBuilder()
                        .setRetainService(RetainServiceROCoProcInput.newBuilder().setBatchMatch(request).build()).build())
                    .build())
            .thenApply(v -> {
                switch (v.getCode()) {
                    case Ok -> {
                        return v.getRoCoProcResult().getRetainService().getBatchMatch();
                    }
                    case TryLater -> throw new TryLaterException();
                    case BadVersion -> throw new BadVersionException();
                    case BadRequest -> throw new BadRequestException();
                    default -> throw new InternalErrorException();
                }
            });
    }
}
