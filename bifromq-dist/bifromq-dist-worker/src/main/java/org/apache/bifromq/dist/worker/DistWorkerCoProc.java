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

package org.apache.bifromq.dist.worker;

import static java.util.Collections.singletonList;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.intersect;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isNULLRange;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.Comparators.FilterLevelsComparator;
import static org.apache.bifromq.dist.worker.Comparators.RouteMatcherComparator;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.buildGroupMatchRoute;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.buildMatchRoute;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.buildNormalMatchRoute;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.dist.rpc.proto.BatchDistReply;
import org.apache.bifromq.dist.rpc.proto.BatchDistRequest;
import org.apache.bifromq.dist.rpc.proto.BatchMatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchMatchRequest;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import org.apache.bifromq.dist.rpc.proto.DistPack;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcOutput;
import org.apache.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import org.apache.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import org.apache.bifromq.dist.rpc.proto.Fact;
import org.apache.bifromq.dist.rpc.proto.GCReply;
import org.apache.bifromq.dist.rpc.proto.GCRequest;
import org.apache.bifromq.dist.rpc.proto.GlobalFilterLevels;
import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.dist.rpc.proto.TopicFanout;
import org.apache.bifromq.dist.worker.cache.ISubscriptionCache;
import org.apache.bifromq.dist.worker.cache.task.AddRoutesTask;
import org.apache.bifromq.dist.worker.cache.task.RemoveRoutesTask;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.dist.worker.schema.cache.RouteDetail;
import org.apache.bifromq.dist.worker.schema.cache.RouteDetailCache;
import org.apache.bifromq.dist.worker.schema.cache.RouteGroupCache;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.BSUtil;

@Slf4j
class DistWorkerCoProc implements IKVRangeCoProc {
    private final Supplier<IKVRangeRefreshableReader> readerProvider;
    private final ISubscriptionCache routeCache;
    private final ITenantsStats tenantsState;
    private final IDeliverExecutorGroup deliverExecutorGroup;
    private final ISubscriptionCleaner subscriptionChecker;
    private transient Fact fact;
    private transient Boundary boundary;

    public DistWorkerCoProc(KVRangeId id,
                            Supplier<IKVRangeRefreshableReader> refreshableReaderProvider,
                            ISubscriptionCache routeCache,
                            ITenantsStats tenantsState,
                            IDeliverExecutorGroup deliverExecutorGroup,
                            ISubscriptionCleaner subscriptionChecker) {
        this.readerProvider = refreshableReaderProvider;
        this.routeCache = routeCache;
        this.tenantsState = tenantsState;
        this.deliverExecutorGroup = deliverExecutorGroup;
        this.subscriptionChecker = subscriptionChecker;
    }

    @Override
    public CompletableFuture<ROCoProcOutput> query(ROCoProcInput input, IKVRangeReader reader) {
        try {
            DistServiceROCoProcInput coProcInput = input.getDistService();
            switch (coProcInput.getInputCase()) {
                case BATCHDIST -> {
                    return batchDist(coProcInput.getBatchDist()).thenApply(v -> ROCoProcOutput.newBuilder()
                        .setDistService(DistServiceROCoProcOutput.newBuilder().setBatchDist(v).build()).build());
                }
                case GC -> {
                    return gc(coProcInput.getGc(), reader).thenApply(v -> ROCoProcOutput.newBuilder()
                        .setDistService(DistServiceROCoProcOutput.newBuilder().setGc(v).build()).build());
                }
                default -> {
                    log.error("Unknown co proc type {}", coProcInput.getInputCase());
                    CompletableFuture<ROCoProcOutput> f = new CompletableFuture<>();
                    f.completeExceptionally(
                        new IllegalStateException("Unknown co proc type " + coProcInput.getInputCase()));
                    return f;
                }
            }
        } catch (Throwable e) {
            log.error("Unable to parse ro co-proc", e);
            CompletableFuture<ROCoProcOutput> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("Unable to parse ro co-proc", e));
            return f;
        }
    }

    @SneakyThrows
    @Override
    public Supplier<MutationResult> mutate(RWCoProcInput input,
                                           IKVRangeReader reader,
                                           IKVWriter writer,
                                           boolean isLeader) {
        DistServiceRWCoProcInput coProcInput = input.getDistService();
        log.trace("Receive rw co-proc request\n{}", coProcInput);
        // tenantId -> topicFilter
        NavigableMap<String, NavigableMap<RouteMatcher, Set<Matching>>> addedMatches = Maps.newTreeMap();
        NavigableMap<String, NavigableMap<RouteMatcher, Set<Matching>>> removedMatches = Maps.newTreeMap();
        DistServiceRWCoProcOutput.Builder outputBuilder = DistServiceRWCoProcOutput.newBuilder();
        AtomicReference<Runnable> afterMutate = new AtomicReference<>();
        switch (coProcInput.getTypeCase()) {
            case BATCHMATCH -> {
                BatchMatchReply.Builder replyBuilder = BatchMatchReply.newBuilder();
                afterMutate.set(batchAddRoute(coProcInput.getBatchMatch(), reader, writer, isLeader, addedMatches,
                    removedMatches, replyBuilder));
                outputBuilder.setBatchMatch(replyBuilder.build());
            }
            case BATCHUNMATCH -> {
                BatchUnmatchReply.Builder replyBuilder = BatchUnmatchReply.newBuilder();
                afterMutate.set(
                    batchRemoveRoute(coProcInput.getBatchUnmatch(), reader, writer, isLeader, removedMatches,
                        replyBuilder));
                outputBuilder.setBatchUnmatch(replyBuilder.build());
            }
            default -> {
                // unreachable
            }
        }
        RWCoProcOutput output = RWCoProcOutput.newBuilder().setDistService(outputBuilder.build()).build();
        return () -> {
            if (!addedMatches.isEmpty()) {
                routeCache.refresh(Maps.transformValues(addedMatches, AddRoutesTask::of));
                addedMatches.forEach((tenantId, topicFilters) -> topicFilters.forEach((topicFilter, matchings) -> {
                    if (topicFilter.getType() == RouteMatcher.Type.OrderedShare) {
                        deliverExecutorGroup.refreshOrderedShareSubRoutes(tenantId, topicFilter);
                    }
                }));
            }
            if (!removedMatches.isEmpty()) {
                routeCache.refresh(Maps.transformValues(removedMatches, RemoveRoutesTask::of));
                removedMatches.forEach((tenantId, topicFilters) -> topicFilters.forEach((topicFilter, matchings) -> {
                    if (topicFilter.getType() == RouteMatcher.Type.OrderedShare) {
                        deliverExecutorGroup.refreshOrderedShareSubRoutes(tenantId, topicFilter);
                    }
                }));
            }
            afterMutate.get().run();
            refreshFact(addedMatches, removedMatches,
                coProcInput.getTypeCase() == DistServiceRWCoProcInput.TypeCase.BATCHMATCH);
            return new MutationResult(output, Optional.of(Any.pack(fact)));
        };
    }

    private void refreshFact(NavigableMap<String, NavigableMap<RouteMatcher, Set<Matching>>> added,
                             NavigableMap<String, NavigableMap<RouteMatcher, Set<Matching>>> removed,
                             boolean isAdd) {
        if (added.isEmpty() && removed.isEmpty()) {
            return;
        }
        boolean needRefresh = false;
        if (!fact.hasFirstGlobalFilterLevels() || !fact.hasLastGlobalFilterLevels()) {
            needRefresh = true;
        } else {
            if (isAdd) {
                Map.Entry<String, NavigableMap<RouteMatcher, Set<Matching>>> firstMutation = added.firstEntry();
                Map.Entry<String, NavigableMap<RouteMatcher, Set<Matching>>> lastMutation = added.lastEntry();
                Iterable<String> firstRoute = toGlobalTopicLevels(firstMutation.getKey(),
                    firstMutation.getValue().firstKey());
                Iterable<String> lastRoute = toGlobalTopicLevels(lastMutation.getKey(),
                    lastMutation.getValue().lastKey());
                if (FilterLevelsComparator
                    .compare(firstRoute, fact.getFirstGlobalFilterLevels().getFilterLevelList()) < 0
                    || FilterLevelsComparator
                    .compare(lastRoute, fact.getLastGlobalFilterLevels().getFilterLevelList()) > 0) {
                    needRefresh = true;
                }
            } else {
                Map.Entry<String, NavigableMap<RouteMatcher, Set<Matching>>> firstMutation = removed.firstEntry();
                Map.Entry<String, NavigableMap<RouteMatcher, Set<Matching>>> lastMutation = removed.lastEntry();
                Iterable<String> firstRoute = toGlobalTopicLevels(firstMutation.getKey(),
                    firstMutation.getValue().firstKey());
                Iterable<String> lastRoute = toGlobalTopicLevels(lastMutation.getKey(),
                    lastMutation.getValue().lastKey());
                if (FilterLevelsComparator
                    .compare(firstRoute, fact.getFirstGlobalFilterLevels().getFilterLevelList()) == 0
                    || FilterLevelsComparator
                    .compare(lastRoute, fact.getLastGlobalFilterLevels().getFilterLevelList()) == 0) {
                    needRefresh = true;
                }
            }
        }
        if (needRefresh) {
            try (IKVRangeRefreshableReader reader = readerProvider.get(); IKVIterator itr = reader.iterator()) {
                reader.refresh();
                setFact(itr);
            }
        }
    }

    private Iterable<String> toGlobalTopicLevels(String tenantId, RouteMatcher routeMatcher) {
        return Iterables.concat(singletonList(tenantId), routeMatcher.getFilterLevelList());
    }

    private void setFact(IKVIterator itr) {
        Fact.Builder factBuilder = Fact.newBuilder();
        itr.seekToFirst();
        if (itr.isValid()) {
            RouteDetail firstRouteDetail = RouteDetailCache.get(itr.key());
            factBuilder.setFirstGlobalFilterLevels(GlobalFilterLevels.newBuilder()
                .addFilterLevel(firstRouteDetail.tenantId())
                .addAllFilterLevel(firstRouteDetail.matcher().getFilterLevelList())
                .build());
        }
        itr.seekToLast();
        if (itr.isValid()) {
            RouteDetail lastRouteDetail = RouteDetailCache.get(itr.key());
            factBuilder.setLastGlobalFilterLevels(GlobalFilterLevels.newBuilder()
                .addFilterLevel(lastRouteDetail.tenantId())
                .addAllFilterLevel(lastRouteDetail.matcher().getFilterLevelList())
                .build());
        }
        fact = factBuilder.build();
    }

    @Override
    public Any reset(Boundary boundary) {
        tenantsState.reset();
        try (IKVRangeRefreshableReader reader = readerProvider.get(); IKVIterator itr = reader.iterator()) {
            this.boundary = boundary;
            routeCache.reset(boundary);
            setFact(itr);
        }
        return Any.pack(fact);
    }

    @Override
    public void onLeader(boolean isLeader) {
        tenantsState.toggleMetering(isLeader);
    }

    public void close() {
        tenantsState.close();
        routeCache.close();
        deliverExecutorGroup.shutdown();
    }

    private Runnable batchAddRoute(BatchMatchRequest request,
                                   IKVRangeReader reader,
                                   IKVWriter writer,
                                   boolean isLeader,
                                   Map<String, NavigableMap<RouteMatcher, Set<Matching>>> newMatches,
                                   Map<String, NavigableMap<RouteMatcher, Set<Matching>>> removedMatches,
                                   BatchMatchReply.Builder replyBuilder) {
        replyBuilder.setReqId(request.getReqId());
        Map<String, AtomicInteger> normalRoutesAdded = new HashMap<>();
        Map<String, AtomicInteger> sharedRoutesAdded = new HashMap<>();
        Map<GlobalTopicFilter, Map<MatchRoute, Integer>> groupMatchRecords = new HashMap<>();
        Map<String, BatchMatchReply.TenantBatch.Code[]> resultMap = new HashMap<>();
        request.getRequestsMap().forEach((tenantId, tenantMatchRequest) -> {
            BatchMatchReply.TenantBatch.Code[] codes = resultMap.computeIfAbsent(tenantId,
                k -> new BatchMatchReply.TenantBatch.Code[tenantMatchRequest.getRouteCount()]);
            Set<ByteString> addedMatches = new HashSet<>();
            for (int i = 0; i < tenantMatchRequest.getRouteCount(); i++) {
                MatchRoute route = tenantMatchRequest.getRoute(i);
                long incarnation = route.getIncarnation();
                RouteMatcher requestMatcher = route.getMatcher();
                if (requestMatcher.getType() == RouteMatcher.Type.Normal) {
                    String receiverUrl = toReceiverUrl(route);
                    ByteString normalRouteKey = toNormalRouteKey(tenantId, requestMatcher, receiverUrl);

                    Optional<Long> incarOpt = reader.get(normalRouteKey).map(BSUtil::toLong);
                    if (incarOpt.isEmpty() || incarOpt.get() < incarnation) {
                        RouteDetail routeDetail = RouteDetailCache.get(normalRouteKey);
                        Matching normalMatching = buildNormalMatchRoute(routeDetail, incarnation);
                        writer.put(normalRouteKey, BSUtil.toByteString(incarnation));
                        // match record may be duplicated in the request
                        if (!addedMatches.contains(normalRouteKey) && incarOpt.isEmpty()) {
                            normalRoutesAdded.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                        }
                        // new match record
                        newMatches.computeIfAbsent(tenantId, k -> new TreeMap<>(RouteMatcherComparator))
                            .computeIfAbsent(routeDetail.matcher(), k -> new HashSet<>())
                            .add(normalMatching);
                        if (incarOpt.isPresent()) {
                            Matching replacedMatching = buildNormalMatchRoute(routeDetail, incarOpt.get());
                            removedMatches.computeIfAbsent(tenantId, k -> new TreeMap<>(RouteMatcherComparator))
                                .computeIfAbsent(routeDetail.matcher(), k -> new HashSet<>())
                                .add(replacedMatching);
                        }
                        addedMatches.add(normalRouteKey);
                    }
                    codes[i] = BatchMatchReply.TenantBatch.Code.OK;
                } else {
                    ByteString groupRouteKey = toGroupRouteKey(tenantId, requestMatcher);
                    RouteDetail routeDetail = RouteDetailCache.get(groupRouteKey);
                    groupMatchRecords.computeIfAbsent(new GlobalTopicFilter(tenantId, routeDetail.matcher()),
                        k -> new HashMap<>()).put(route, i);
                }
            }
        });
        groupMatchRecords.forEach((globalTopicFilter, newGroupMembers) -> {
            String tenantId = globalTopicFilter.tenantId;
            RouteMatcher origRouteMatcher = globalTopicFilter.routeMatcher;
            ByteString groupMatchRecordKey = toGroupRouteKey(tenantId, origRouteMatcher);
            RouteGroup.Builder matchGroup = reader.get(groupMatchRecordKey)
                .map(b -> RouteGroupCache.get(b).toBuilder()).orElseGet(() -> {
                    // new shared subscription
                    sharedRoutesAdded.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                    return RouteGroup.newBuilder();
                });
            boolean updated = false;
            int maxMembers = request.getRequestsMap().get(tenantId).getOption().getMaxReceiversPerSharedSubGroup();
            for (MatchRoute route : newGroupMembers.keySet()) {
                int resultIdx = newGroupMembers.get(route);
                String receiverUrl = toReceiverUrl(route);
                if (!matchGroup.containsMembers(receiverUrl)) {
                    if (matchGroup.getMembersCount() < maxMembers) {
                        matchGroup.putMembers(receiverUrl, route.getIncarnation());
                        resultMap.get(tenantId)[resultIdx] = BatchMatchReply.TenantBatch.Code.OK;
                        updated = true;
                    } else {
                        resultMap.get(tenantId)[resultIdx] = BatchMatchReply.TenantBatch.Code.EXCEED_LIMIT;
                    }
                } else {
                    if (matchGroup.getMembersMap().get(receiverUrl) < route.getIncarnation()) {
                        matchGroup.putMembers(receiverUrl, route.getIncarnation());
                        updated = true;
                    }
                    resultMap.get(tenantId)[resultIdx] = BatchMatchReply.TenantBatch.Code.OK;
                }
            }
            if (updated) {
                RouteDetail routeDetail = RouteDetailCache.get(groupMatchRecordKey);
                RouteGroup routeGroup = matchGroup.build();
                Matching groupMatching = buildGroupMatchRoute(routeDetail, routeGroup);
                writer.put(groupMatchRecordKey, routeGroup.toByteString());
                newMatches.computeIfAbsent(tenantId, k -> new TreeMap<>(RouteMatcherComparator))
                    .computeIfAbsent(routeDetail.matcher(), k -> new HashSet<>())
                    .add(groupMatching);
            }
        });
        resultMap.forEach((tenantId, codes) -> {
            BatchMatchReply.TenantBatch.Builder batchBuilder = BatchMatchReply.TenantBatch.newBuilder();
            for (BatchMatchReply.TenantBatch.Code code : codes) {
                batchBuilder.addCode(code);
            }
            replyBuilder.putResults(tenantId, batchBuilder.build());
        });
        return () -> {
            normalRoutesAdded.forEach((tenantId, added) ->
                tenantsState.incNormalRoutes(tenantId, added.get()));
            sharedRoutesAdded.forEach((tenantId, added) ->
                tenantsState.incSharedRoutes(tenantId, added.get()));
            tenantsState.toggleMetering(isLeader);
        };
    }

    private Runnable batchRemoveRoute(BatchUnmatchRequest request,
                                      IKVRangeReader reader,
                                      IKVWriter writer,
                                      boolean isLeader,
                                      Map<String, NavigableMap<RouteMatcher, Set<Matching>>> removedMatches,
                                      BatchUnmatchReply.Builder replyBuilder) {
        replyBuilder.setReqId(request.getReqId());
        Map<String, AtomicInteger> normalRoutesRemoved = new HashMap<>();
        Map<String, AtomicInteger> sharedRoutesRemoved = new HashMap<>();
        Map<GlobalTopicFilter, Map<MatchRoute, Integer>> delGroupMatchRecords = new HashMap<>();
        Map<String, BatchUnmatchReply.TenantBatch.Code[]> resultMap = new HashMap<>();
        request.getRequestsMap().forEach((tenantId, tenantUnmatchRequest) -> {
            BatchUnmatchReply.TenantBatch.Code[] codes = resultMap.computeIfAbsent(tenantId,
                k -> new BatchUnmatchReply.TenantBatch.Code[tenantUnmatchRequest.getRouteCount()]);
            Set<ByteString> delMatches = new HashSet<>();
            for (int i = 0; i < tenantUnmatchRequest.getRouteCount(); i++) {
                MatchRoute route = tenantUnmatchRequest.getRoute(i);
                RouteMatcher requestMatcher = route.getMatcher();
                if (requestMatcher.getType() == RouteMatcher.Type.Normal) {
                    String receiverUrl = toReceiverUrl(route);
                    ByteString normalRouteKey = toNormalRouteKey(tenantId, requestMatcher, receiverUrl);
                    Optional<Long> incarOpt = reader.get(normalRouteKey).map(BSUtil::toLong);
                    if (incarOpt.isPresent() && incarOpt.get() <= route.getIncarnation()) {
                        RouteDetail routeDetail = RouteDetailCache.get(normalRouteKey);
                        Matching normalMatching = buildNormalMatchRoute(routeDetail, incarOpt.get());
                        writer.delete(normalRouteKey);
                        if (!delMatches.contains(normalRouteKey)) {
                            normalRoutesRemoved.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                        }
                        removedMatches.computeIfAbsent(tenantId, k -> new TreeMap<>(RouteMatcherComparator))
                            .computeIfAbsent(routeDetail.matcher(), k -> new HashSet<>())
                            .add(normalMatching);
                        delMatches.add(normalRouteKey);
                        codes[i] = BatchUnmatchReply.TenantBatch.Code.OK;
                    } else {
                        codes[i] = BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED;
                    }
                } else {
                    ByteString groupRouteKey = toGroupRouteKey(tenantId, requestMatcher);
                    RouteDetail routeDetail = RouteDetailCache.get(groupRouteKey);
                    delGroupMatchRecords.computeIfAbsent(new GlobalTopicFilter(tenantId, routeDetail.matcher()),
                        k -> new HashMap<>()).put(route, i);
                }
            }
        });
        delGroupMatchRecords.forEach((globalTopicFilter, delGroupMembers) -> {
            String tenantId = globalTopicFilter.tenantId;
            RouteMatcher origRouteMatcher = globalTopicFilter.routeMatcher;
            ByteString groupRouteKey = toGroupRouteKey(tenantId, origRouteMatcher);
            Optional<ByteString> value = reader.get(groupRouteKey);
            if (value.isPresent()) {
                Matching matching = buildMatchRoute(groupRouteKey, value.get());
                assert matching instanceof GroupMatching;
                GroupMatching groupMatching = (GroupMatching) matching;
                Map<String, Long> existing = Maps.newHashMap(groupMatching.receivers());
                delGroupMembers.forEach((route, resultIdx) -> {
                    String receiverUrl = toReceiverUrl(route);
                    if (existing.containsKey(receiverUrl) && existing.get(receiverUrl) <= route.getIncarnation()) {
                        existing.remove(receiverUrl);
                        resultMap.get(tenantId)[resultIdx] = BatchUnmatchReply.TenantBatch.Code.OK;
                    } else {
                        resultMap.get(tenantId)[resultIdx] = BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED;
                    }
                });
                if (existing.size() != groupMatching.receivers().size()) {
                    if (existing.isEmpty()) {
                        writer.delete(groupRouteKey);
                        sharedRoutesRemoved.computeIfAbsent(tenantId, k -> new AtomicInteger()).incrementAndGet();
                    } else {
                        writer.put(groupRouteKey,
                            RouteGroup.newBuilder().putAllMembers(existing).build().toByteString());
                    }
                    RouteDetail routeDetail = RouteDetailCache.get(groupRouteKey);
                    RouteGroup routeGroup = RouteGroup.newBuilder().putAllMembers(existing).build();
                    Matching newGroupMatching = buildGroupMatchRoute(routeDetail, routeGroup);
                    removedMatches.computeIfAbsent(tenantId, k -> new TreeMap<>(RouteMatcherComparator))
                        .computeIfAbsent(routeDetail.matcher(), k -> new HashSet<>())
                        .add(newGroupMatching);
                }
            } else {
                delGroupMembers.forEach((detail, resultIdx) -> resultMap.get(tenantId)[resultIdx] =
                    BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED);
            }
        });
        resultMap.forEach((tenantId, codes) -> {
            BatchUnmatchReply.TenantBatch.Builder batchBuilder = BatchUnmatchReply.TenantBatch.newBuilder();
            for (BatchUnmatchReply.TenantBatch.Code code : codes) {
                batchBuilder.addCode(code);
            }
            replyBuilder.putResults(tenantId, batchBuilder.build());
        });
        return () -> {
            normalRoutesRemoved.forEach(
                (tenantId, removed) -> tenantsState.decNormalRoutes(tenantId, removed.get()));
            sharedRoutesRemoved.forEach(
                (tenantId, removed) -> tenantsState.decSharedRoutes(tenantId, removed.get()));
            tenantsState.toggleMetering(isLeader);
        };
    }

    private CompletableFuture<BatchDistReply> batchDist(BatchDistRequest request) {
        List<DistPack> distPackList = request.getDistPackList();
        if (distPackList.isEmpty()) {
            return CompletableFuture.completedFuture(BatchDistReply.newBuilder().setReqId(request.getReqId()).build());
        }
        List<CompletableFuture<Void>> distFutures = new LinkedList<>();
        ConcurrentMap<String, Map<String, AtomicInteger>> tenantTopicFanOuts = new ConcurrentHashMap<>();
        for (DistPack distPack : distPackList) {
            String tenantId = distPack.getTenantId();
            Map<String, AtomicInteger> topicFanouts = tenantTopicFanOuts.computeIfAbsent(tenantId,
                (k) -> new ConcurrentHashMap<>());
            ByteString tenantStartKey = tenantBeginKey(tenantId);
            Boundary tenantBoundary = intersect(toBoundary(tenantStartKey, upperBound(tenantStartKey)), boundary);
            if (isNULLRange(tenantBoundary)) {
                continue;
            }
            for (TopicMessagePack topicMsgPack : distPack.getMsgPackList()) {
                String topic = topicMsgPack.getTopic();
                AtomicInteger fanout = topicFanouts.computeIfAbsent(topic, k -> new AtomicInteger());
                distFutures.add(routeCache.get(tenantId, topic)
                    .thenAccept(routes -> {
                        deliverExecutorGroup.submit(tenantId, routes, topicMsgPack);
                        fanout.addAndGet(routes.size());
                    }));
            }
        }
        return CompletableFuture.allOf(distFutures.toArray(new CompletableFuture[distFutures.size()]))
            .thenApply(v -> {
                // tenantId -> topic -> fanOut
                BatchDistReply.Builder replyBuilder = BatchDistReply.newBuilder().setReqId(request.getReqId());
                tenantTopicFanOuts.forEach((k, f) -> {
                    TopicFanout.Builder fanoutBuilder = TopicFanout.newBuilder();
                    f.forEach((topic, count) -> fanoutBuilder.putFanout(topic, count.get()));
                    replyBuilder.putResult(k, fanoutBuilder.build());
                });
                return replyBuilder.build();
            });
    }

    private CompletableFuture<GCReply> gc(GCRequest request, IKVRangeReader reader) {
        int stepUsed = Math.max(request.getStepHint(), 1);
        int scanQuota = request.getScanQuota() > 0 ? request.getScanQuota() : 256 * stepUsed;

        // subBrokerId -> delivererKey -> tenantId-> CheckRequest
        Map<Integer, Map<String, Map<String, CheckRequest.Builder>>> checkRequestBuilders = new HashMap<>();

        try (IKVIterator itr = reader.iterator()) {
            // clamp start key to current boundary when provided
            if (request.hasStartKey()) {
                ByteString startKey = request.getStartKey();
                if (boundary != null) {
                    ByteString startBoundary = BoundaryUtil.startKey(boundary);
                    ByteString endBoundary = BoundaryUtil.endKey(boundary);
                    if (BoundaryUtil.compareStartKey(startKey, startBoundary) < 0) {
                        startKey = startBoundary;
                    } else if (BoundaryUtil.compareEndKeys(startKey, endBoundary) >= 0) {
                        // clamp to start when beyond end
                        startKey = startBoundary;
                    }
                }
                if (startKey != null) {
                    itr.seek(startKey);
                } else {
                    itr.seekToFirst();
                }
            } else {
                itr.seekToFirst();
            }

            // if range is empty, return immediately
            if (!itr.isValid()) {
                return CompletableFuture.completedFuture(GCReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setInspectedCount(0)
                    .setRemoveSuccess(0)
                    .setWrapped(false)
                    .build());
            }

            AtomicInteger inspectedCount = new AtomicInteger();
            AtomicBoolean wrapped = new AtomicBoolean(false);
            ByteString sessionStartKey = null;

            outer:
            while (true) {
                if (!itr.isValid()) {
                    // reach tail
                    if (!wrapped.get()) {
                        itr.seekToFirst();
                        if (!itr.isValid()) {
                            break; // still empty
                        }
                        wrapped.set(true);
                    } else {
                        break;
                    }
                }

                ByteString currentKey = itr.key();
                // stop if met sessionStartKey after wrap before decoding
                if (wrapped.get() && currentKey.equals(sessionStartKey)) {
                    break;
                }

                if (sessionStartKey == null) {
                    sessionStartKey = currentKey;
                }
                Matching matching = buildMatchRoute(currentKey, itr.value());
                switch (matching.type()) {
                    case Normal -> {
                        if (!routeCache.isCached(matching.tenantId(), matching.matcher.getFilterLevelList())) {
                            NormalMatching normalMatching = ((NormalMatching) matching);
                            checkRequestBuilders.computeIfAbsent(normalMatching.subBrokerId(), k -> new HashMap<>())
                                .computeIfAbsent(normalMatching.delivererKey(), k -> new HashMap<>())
                                .computeIfAbsent(normalMatching.tenantId(), k -> CheckRequest.newBuilder()
                                    .setTenantId(k)
                                    .setDelivererKey(normalMatching.delivererKey()))
                                .addMatchInfo(((NormalMatching) matching).matchInfo());
                        }
                    }
                    case Group -> {
                        GroupMatching groupMatching = ((GroupMatching) matching);
                        if (!routeCache.isCached(groupMatching.tenantId(), matching.matcher.getFilterLevelList())) {
                            for (NormalMatching normalMatching : groupMatching.receiverList) {
                                checkRequestBuilders.computeIfAbsent(normalMatching.subBrokerId(), k -> new HashMap<>())
                                    .computeIfAbsent(normalMatching.delivererKey(), k -> new HashMap<>())
                                    .computeIfAbsent(normalMatching.tenantId(), k -> CheckRequest.newBuilder()
                                        .setTenantId(k)
                                        .setDelivererKey(normalMatching.delivererKey()))
                                    .addMatchInfo(normalMatching.matchInfo());
                            }
                        }
                    }
                    default -> {
                        // never happen
                    }
                }
                inspectedCount.incrementAndGet();
                if (inspectedCount.get() >= scanQuota) {
                    // stop by quota
                    itr.next();
                    break;
                }

                // skip over stepUsed-1 entries
                int skip = stepUsed - 1;
                while (skip-- > 0) {
                    itr.next();
                    if (!itr.isValid()) {
                        // let outer loop handle wrap or stop
                        continue outer;
                    }
                }
                // move to next for next iteration
                itr.next();
            }

            // aggregate sweep results
            List<CompletableFuture<ISubscriptionCleaner.GCStats>> checkFutures = new ArrayList<>();
            for (int subBrokerId : checkRequestBuilders.keySet()) {
                for (String delivererKey : checkRequestBuilders.get(subBrokerId).keySet()) {
                    for (Map.Entry<String, CheckRequest.Builder> entry : checkRequestBuilders.get(subBrokerId)
                        .get(delivererKey).entrySet()) {
                        checkFutures.add(subscriptionChecker.sweep(subBrokerId, entry.getValue().build()));
                    }
                }
            }

            CompletableFuture<Void> all = CompletableFuture.allOf(checkFutures.toArray(CompletableFuture[]::new));
            return all.thenApply(v -> {
                int success = 0;
                for (CompletableFuture<ISubscriptionCleaner.GCStats> f : checkFutures) {
                    success += f.join().success();
                }
                GCReply.Builder reply = GCReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setInspectedCount(inspectedCount.get())
                    .setRemoveSuccess(success)
                    .setWrapped(wrapped.get());
                if (itr.isValid()) {
                    reply.setNextStartKey(itr.key());
                }
                return reply.build();
            });
        }
    }

    private record GlobalTopicFilter(String tenantId, RouteMatcher routeMatcher) {
    }
}
