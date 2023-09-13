/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.dist.server.scheduler;

import static com.baidu.bifromq.dist.entity.EntityUtil.matchRecordKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.subInfoKey;
import static com.baidu.bifromq.dist.entity.EntityUtil.toQualifiedInboxId;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_BURST_LATENCY_MS;
import static com.baidu.bifromq.sysprops.BifroMQSysProp.CONTROL_PLANE_TOLERABLE_LATENCY_MS;

import com.baidu.bifromq.basekv.KVRangeSetting;
import com.baidu.bifromq.basekv.client.IBaseKVStoreClient;
import com.baidu.bifromq.basekv.store.proto.KVRangeRWRequest;
import com.baidu.bifromq.basekv.store.proto.ReplyCode;
import com.baidu.bifromq.basescheduler.BatchCallScheduler;
import com.baidu.bifromq.basescheduler.Batcher;
import com.baidu.bifromq.basescheduler.CallTask;
import com.baidu.bifromq.basescheduler.IBatchCall;
import com.baidu.bifromq.dist.rpc.proto.ClearRequest;
import com.baidu.bifromq.dist.rpc.proto.ClearSubInfo;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcInput;
import com.baidu.bifromq.dist.rpc.proto.DistServiceRWCoProcOutput;
import com.baidu.bifromq.dist.rpc.proto.GroupMatchRecord;
import com.baidu.bifromq.dist.rpc.proto.InboxSubInfo;
import com.baidu.bifromq.dist.rpc.proto.LeaveMatchGroup;
import com.baidu.bifromq.dist.rpc.proto.QInboxIdList;
import com.baidu.bifromq.dist.rpc.proto.SubRequest;
import com.baidu.bifromq.dist.rpc.proto.TopicFilterList;
import com.baidu.bifromq.dist.rpc.proto.UnsubRequest;
import com.baidu.bifromq.dist.rpc.proto.UpdateRequest;
import com.baidu.bifromq.type.QoS;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.maps.NonBlockingHashSet;

@Slf4j
public class SubCallScheduler extends BatchCallScheduler<SubCall, SubCallResult, KVRangeSetting>
    implements ISubCallScheduler {
    private final IBaseKVStoreClient distWorkerClient;

    public SubCallScheduler(IBaseKVStoreClient distWorkerClient) {
        super("dist_server_update_batcher", Duration.ofMillis(CONTROL_PLANE_TOLERABLE_LATENCY_MS.get()),
            Duration.ofMillis(CONTROL_PLANE_BURST_LATENCY_MS.get()));
        this.distWorkerClient = distWorkerClient;
    }

    @Override
    protected Batcher<SubCall, SubCallResult, KVRangeSetting> newBatcher(String name,
                                                                         long tolerableLatencyNanos,
                                                                         long burstLatencyNanos,
                                                                         KVRangeSetting range) {
        return new SubCallBatcher(name, tolerableLatencyNanos, burstLatencyNanos, range, distWorkerClient);
    }

    @Override
    protected Optional<KVRangeSetting> find(SubCall subCall) {
        return distWorkerClient.findByKey(rangeKey(subCall));
    }

    private ByteString rangeKey(SubCall call) {
        switch (call.type()) {
            case ADD_TOPIC_FILTER -> {
                SubRequest request = ((SubCall.AddTopicFilter) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return subInfoKey(request.getTenantId(), qInboxId);
            }
            case INSERT_MATCH_RECORD -> {
                SubRequest request = ((SubCall.InsertMatchRecord) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return matchRecordKey(request.getTenantId(), request.getTopicFilter(), qInboxId);
            }
            case JOIN_MATCH_GROUP -> {
                SubRequest request = ((SubCall.JoinMatchGroup) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return matchRecordKey(request.getTenantId(), request.getTopicFilter(), qInboxId);
            }
            case REMOVE_TOPIC_FILTER -> {
                UnsubRequest request = ((SubCall.RemoveTopicFilter) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return subInfoKey(request.getTenantId(), qInboxId);
            }
            case DELETE_MATCH_RECORD -> {
                UnsubRequest request = ((SubCall.DeleteMatchRecord) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return matchRecordKey(request.getTenantId(), request.getTopicFilter(), qInboxId);
            }
            case LEAVE_JOIN_GROUP -> {
                UnsubRequest request = ((SubCall.LeaveJoinGroup) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return matchRecordKey(request.getTenantId(), request.getTopicFilter(), qInboxId);
            }
            case CLEAR -> {
                ClearRequest request = ((SubCall.Clear) call).request;
                String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                    request.getDelivererKey());
                return subInfoKey(request.getTenantId(), qInboxId);
            }
            default -> throw new UnsupportedOperationException("Unsupported request type: " + call.type());
        }
    }

    private static class SubCallBatcher extends Batcher<SubCall, SubCallResult, KVRangeSetting> {

        private class SubCallBatch implements IBatchCall<SubCall, SubCallResult> {

            // key: subInfoKeyUtf8, subKey: topicFilter
            private final Map<String, Map<String, QoS>> addTopicFilter = new HashMap<>();

            // key: subInfoKeyUtf8, subKey: topicFilter
            private final Map<String, Map<String, CompletableFuture<SubCallResult>>> onAddTopicFilter = new HashMap<>();

            // key: subInfoKeyUtf8, subKey: topicFilter
            private final Map<String, Set<String>> remTopicFilter = new HashMap<>();

            // key: subInfoKeyUtf8, subKey: topicFilter
            private final Map<String, Map<String, CompletableFuture<SubCallResult>>> onRemTopicFilter = new HashMap<>();

            // key: normal matchRecordKey
            private final Map<String, QoS> insertMatchRecord = new HashMap<>();

            // key: normal matchRecordKey
            private final Map<String, CompletableFuture<SubCallResult>> onInsertMatchRecord =
                new HashMap<>();

            // key: group matchRecordKey
            private final Map<String, Map<String, QoS>> joinMatchGroup = new HashMap<>();

            // key: group matchRecordKey, subKey: qInboxId
            private final Map<String, Map<String, CompletableFuture<SubCallResult>>> onJoinMatchGroup = new HashMap<>();

            // elem: normal matchRecordKey
            private final Set<String> delMatchRecord = new HashSet<>();

            // key: normal matchRecordKey
            private final Map<String, CompletableFuture<SubCallResult>> onDelMatchRecord = new HashMap<>();

            // key: group matchRecordKey value: set of qualified inboxId
            private final Map<String, Set<String>> leaveMatchGroup = new HashMap<>();

            // key: group matchRecordKey
            private final Map<String, CompletableFuture<SubCallResult>> onLeaveMatchGroup = new HashMap<>();

            // key: subInfoKey
            private final Set<ByteString> clearSubInfo = new HashSet<>();

            // key: subInfoKey
            private final Map<ByteString, CompletableFuture<SubCallResult>> onClearSubInfo = new HashMap<>();

            @Override
            public void reset() {
                addTopicFilter.clear();
                onAddTopicFilter.clear();
                remTopicFilter.clear();
                onRemTopicFilter.clear();
                insertMatchRecord.clear();
                onInsertMatchRecord.clear();
                joinMatchGroup.clear();
                onJoinMatchGroup.clear();
                delMatchRecord.clear();
                onDelMatchRecord.clear();
                leaveMatchGroup.clear();
                onLeaveMatchGroup.clear();
                clearSubInfo.clear();
                onClearSubInfo.clear();
            }

            @Override
            public void add(CallTask<SubCall, SubCallResult> callTask) {
                SubCall subCall = callTask.call;
                switch (subCall.type()) {
                    case ADD_TOPIC_FILTER -> {
                        SubRequest request = ((SubCall.AddTopicFilter) subCall).request;
                        String subInfoKeyUtf8 = subInfoKey(request.getTenantId(),
                            toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                                request.getDelivererKey())).toStringUtf8();
                        addTopicFilter.computeIfAbsent(subInfoKeyUtf8, k -> new NonBlockingHashMap<>())
                            .putIfAbsent(request.getTopicFilter(), request.getSubQoS());
                        onAddTopicFilter.computeIfAbsent(subInfoKeyUtf8, k -> new NonBlockingHashMap<>())
                            .put(request.getTopicFilter(), callTask.callResult);
                    }
                    case INSERT_MATCH_RECORD -> {
                        SubRequest request = ((SubCall.InsertMatchRecord) subCall).request;
                        String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                            request.getDelivererKey());
                        String matchRecordKeyUtf8 = matchRecordKey(request.getTenantId(),
                            request.getTopicFilter(), qInboxId).toStringUtf8();
                        insertMatchRecord.put(matchRecordKeyUtf8, request.getSubQoS());
                        onInsertMatchRecord.put(matchRecordKeyUtf8, callTask.callResult);
                    }
                    case JOIN_MATCH_GROUP -> {
                        SubRequest request = ((SubCall.JoinMatchGroup) subCall).request;
                        String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                            request.getDelivererKey());
                        String matchRecordKeyUtf8 = matchRecordKey(request.getTenantId(),
                            request.getTopicFilter(), qInboxId).toStringUtf8();
                        joinMatchGroup.computeIfAbsent(matchRecordKeyUtf8, k -> new NonBlockingHashMap<>())
                            .put(qInboxId, request.getSubQoS());
                        onJoinMatchGroup.computeIfAbsent(matchRecordKeyUtf8, k -> new NonBlockingHashMap<>())
                            .put(qInboxId, callTask.callResult);
                    }
                    case REMOVE_TOPIC_FILTER -> {
                        UnsubRequest request = ((SubCall.RemoveTopicFilter) subCall).request;
                        String subInfoKeyUtf8 = subInfoKey(request.getTenantId(),
                            toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                                request.getDelivererKey())).toStringUtf8();
                        remTopicFilter.computeIfAbsent(subInfoKeyUtf8, k -> new NonBlockingHashSet<>())
                            .add(request.getTopicFilter());
                        onRemTopicFilter.computeIfAbsent(subInfoKeyUtf8, k -> new NonBlockingHashMap<>())
                            .put(request.getTopicFilter(), callTask.callResult);
                    }
                    case DELETE_MATCH_RECORD -> {
                        UnsubRequest request = ((SubCall.DeleteMatchRecord) subCall).request;
                        String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                            request.getDelivererKey());
                        String matchRecordKeyUtf8 = matchRecordKey(request.getTenantId(),
                            request.getTopicFilter(), qInboxId).toStringUtf8();
                        delMatchRecord.add(matchRecordKeyUtf8);
                        onDelMatchRecord.put(matchRecordKeyUtf8, callTask.callResult);
                    }
                    case LEAVE_JOIN_GROUP -> {
                        UnsubRequest request = ((SubCall.LeaveJoinGroup) subCall).request;
                        String qInboxId = toQualifiedInboxId(request.getBroker(), request.getInboxId(),
                            request.getDelivererKey());
                        String matchRecordKeyUtf8 = matchRecordKey(request.getTenantId(),
                            request.getTopicFilter(), qInboxId).toStringUtf8();
                        leaveMatchGroup.computeIfAbsent(matchRecordKeyUtf8, k -> new NonBlockingHashSet<>())
                            .add(qInboxId);
                        onLeaveMatchGroup.put(matchRecordKeyUtf8, callTask.callResult);
                    }
                    case CLEAR -> {
                        ClearRequest request = ((SubCall.Clear) subCall).request;
                        ByteString subInfoKey = subInfoKey(request.getTenantId(),
                            toQualifiedInboxId(request.getBroker(), request.getInboxId(), request.getDelivererKey()));
                        clearSubInfo.add(subInfoKey);
                        onClearSubInfo.put(subInfoKey, callTask.callResult);
                    }
                    default -> throw new UnsupportedOperationException("Unsupported request type: " + subCall.type());
                }
            }

            @Override
            public CompletableFuture<Void> execute() {
                UpdateRequest.Builder reqBuilder = UpdateRequest.newBuilder().setReqId(System.nanoTime());
                if (!addTopicFilter.isEmpty()) {
                    reqBuilder.setAddTopicFilter(com.baidu.bifromq.dist.rpc.proto.AddTopicFilter.newBuilder()
                        .putAllTopicFilter(Maps.transformValues(addTopicFilter,
                            e -> InboxSubInfo.newBuilder().putAllTopicFilters(e).build()))
                        .build());
                }
                if (!remTopicFilter.isEmpty()) {
                    reqBuilder.setRemoveTopicFilter(com.baidu.bifromq.dist.rpc.proto.RemoveTopicFilter.newBuilder()
                        .putAllTopicFilter(Maps.transformValues(remTopicFilter,
                            e -> TopicFilterList.newBuilder().addAllTopicFilter(e).build()))
                        .build());
                }
                if (!insertMatchRecord.isEmpty()) {
                    reqBuilder.setInsertMatchRecord(com.baidu.bifromq.dist.rpc.proto.InsertMatchRecord.newBuilder()
                        .putAllRecord(insertMatchRecord)
                        .build());
                }
                if (!joinMatchGroup.isEmpty()) {
                    reqBuilder.setJoinMatchGroup(com.baidu.bifromq.dist.rpc.proto.JoinMatchGroup.newBuilder()
                        .putAllRecord(Maps.transformValues(joinMatchGroup,
                            e -> GroupMatchRecord.newBuilder().putAllEntry(e).build()))
                        .build());
                }
                if (!delMatchRecord.isEmpty()) {
                    reqBuilder.setDeleteMatchRecord(com.baidu.bifromq.dist.rpc.proto.DeleteMatchRecord.newBuilder()
                        .addAllMatchRecordKey(delMatchRecord)
                        .build());
                }
                if (!leaveMatchGroup.isEmpty()) {
                    reqBuilder.setLeaveMatchGroup(LeaveMatchGroup.newBuilder()
                        .putAllRecord(Maps.transformValues(leaveMatchGroup,
                            e -> QInboxIdList.newBuilder().addAllQInboxId(e).build()))
                        .build());
                }
                if (!clearSubInfo.isEmpty()) {
                    reqBuilder.setClearSubInfo(ClearSubInfo.newBuilder().addAllSubInfoKey(clearSubInfo).build());
                }
                UpdateRequest request = reqBuilder.build();
                return distWorkerClient.execute(range.leader, KVRangeRWRequest.newBuilder()
                    .setReqId(request.getReqId())
                    .setVer(range.ver)
                    .setKvRangeId(range.id)
                    .setRwCoProc(
                        DistServiceRWCoProcInput.newBuilder().setUpdateRequest(request).build().toByteString())
                    .build()).thenApply(reply -> {
                    if (reply.getCode() == ReplyCode.Ok) {
                        try {
                            return DistServiceRWCoProcOutput.parseFrom(reply.getRwCoProcResult()).getUpdateReply();
                        } catch (InvalidProtocolBufferException e) {
                            log.error("Unable to parse rw co-proc output", e);
                            throw new RuntimeException(e);
                        }
                    }
                    log.warn("Failed to exec rw co-proc[code={}]", reply.getCode());
                    throw new RuntimeException();
                }).thenAccept(reply -> {
                    for (String subInfoKeyUtf8 : onAddTopicFilter.keySet()) {
                        Map<String, CompletableFuture<SubCallResult>> addResults = onAddTopicFilter.get(subInfoKeyUtf8);
                        for (String qInboxId : addResults.keySet()) {
                            addResults.get(qInboxId)
                                .complete(new SubCallResult.AddTopicFilterResult(reply.getAddTopicFilter()
                                    .getResultMap()
                                    .get(subInfoKeyUtf8)
                                    .getResultsMap()
                                    .get(qInboxId)));
                        }
                    }
                    for (String subInfoKeyUtf8 : onRemTopicFilter.keySet()) {
                        Map<String, CompletableFuture<SubCallResult>> remResults = onRemTopicFilter.get(subInfoKeyUtf8);
                        for (String qInboxId : remResults.keySet()) {
                            remResults.get(qInboxId)
                                .complete(new SubCallResult.RemoveTopicFilterResult(reply.getRemoveTopicFilter()
                                    .getResultMap()
                                    .get(subInfoKeyUtf8)
                                    .getResultMap()
                                    .get(qInboxId)));
                        }
                    }

                    for (String matchRecordKeyUtf8 : onInsertMatchRecord.keySet()) {
                        onInsertMatchRecord.get(matchRecordKeyUtf8)
                            .complete(new SubCallResult.InsertMatchRecordResult());
                    }

                    for (String matchRecordKeyUtf8 : onDelMatchRecord.keySet()) {
                        onDelMatchRecord.get(matchRecordKeyUtf8)
                            .complete(new SubCallResult.DeleteMatchRecordResult(reply.getDeleteMatchRecord()
                                .getExistOrDefault(matchRecordKeyUtf8, false)));
                    }

                    for (String matchRecordKeyUtf8 : onJoinMatchGroup.keySet()) {
                        for (String qInboxId : onJoinMatchGroup.get(matchRecordKeyUtf8).keySet()) {
                            onJoinMatchGroup.get(matchRecordKeyUtf8)
                                .get(qInboxId)
                                .complete(new SubCallResult.JoinMatchGroupResult(reply.getJoinMatchGroup()
                                    .getResultMap()
                                    .get(matchRecordKeyUtf8)
                                    .getResultMap()
                                    .get(qInboxId)));
                        }
                    }

                    for (String matchRecordKeyUtf8 : onLeaveMatchGroup.keySet()) {
                        onLeaveMatchGroup.get(matchRecordKeyUtf8)
                            .complete(new SubCallResult.LeaveJoinGroupResult());
                    }

                    for (int i = 0; i < request.getClearSubInfo().getSubInfoKeyCount(); i++) {
                        ByteString subInfoKey = request.getClearSubInfo().getSubInfoKey(i);
                        onClearSubInfo.get(subInfoKey)
                            .complete(new SubCallResult.ClearResult(reply.getClearSubInfo().getSubInfo(i)));
                    }
                }).exceptionally(e -> {
                    for (String subInfoKeyUtf8 : onAddTopicFilter.keySet()) {
                        Map<String, CompletableFuture<SubCallResult>> addResults = onAddTopicFilter.get(subInfoKeyUtf8);
                        for (String qInboxId : addResults.keySet()) {
                            addResults.get(qInboxId).completeExceptionally(e);
                        }
                    }
                    for (String subInfoKeyUtf8 : onRemTopicFilter.keySet()) {
                        Map<String, CompletableFuture<SubCallResult>> remResults = onRemTopicFilter.get(subInfoKeyUtf8);
                        for (String qInboxId : remResults.keySet()) {
                            remResults.get(qInboxId).completeExceptionally(e);
                        }
                    }
                    for (String matchRecordKeyUtf8 : onInsertMatchRecord.keySet()) {
                        onInsertMatchRecord.get(matchRecordKeyUtf8).completeExceptionally(e);
                    }

                    for (String matchRecordKeyUtf8 : onDelMatchRecord.keySet()) {
                        onDelMatchRecord.get(matchRecordKeyUtf8).completeExceptionally(e);
                    }

                    for (String matchRecordKeyUtf8 : onJoinMatchGroup.keySet()) {
                        for (String qInboxId : onJoinMatchGroup.get(matchRecordKeyUtf8).keySet()) {
                            onJoinMatchGroup.get(matchRecordKeyUtf8).get(qInboxId).completeExceptionally(e);
                        }
                    }

                    for (String matchRecordKeyUtf8 : onLeaveMatchGroup.keySet()) {
                        onLeaveMatchGroup.get(matchRecordKeyUtf8).completeExceptionally(e);
                    }

                    for (int i = 0; i < request.getClearSubInfo().getSubInfoKeyCount(); i++) {
                        ByteString subInfoKey = request.getClearSubInfo().getSubInfoKey(i);
                        onClearSubInfo.get(subInfoKey).completeExceptionally(e);
                    }
                    return null;
                });
            }
        }

        private final IBaseKVStoreClient distWorkerClient;
        private final KVRangeSetting range;

        private SubCallBatcher(String name,
                               long tolerableLatencyNanos,
                               long burstLatencyNanos,
                               KVRangeSetting range,
                               IBaseKVStoreClient distWorkerClient) {
            super(range, name, tolerableLatencyNanos, burstLatencyNanos);
            this.distWorkerClient = distWorkerClient;
            this.range = range;
        }

        @Override
        protected IBatchCall<SubCall, SubCallResult> newBatch() {
            return new SubCallBatch();
        }
    }
}
