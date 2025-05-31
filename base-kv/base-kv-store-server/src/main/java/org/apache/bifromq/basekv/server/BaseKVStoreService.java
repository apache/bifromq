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

package org.apache.bifromq.basekv.server;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.baserpc.server.UnaryResponse.response;

import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basekv.metaservice.IBaseKVClusterMetadataManager;
import org.apache.bifromq.basekv.store.IKVRangeStore;
import org.apache.bifromq.basekv.store.KVRangeStore;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc;
import org.apache.bifromq.basekv.store.proto.BootstrapReply;
import org.apache.bifromq.basekv.store.proto.BootstrapRequest;
import org.apache.bifromq.basekv.store.proto.ChangeReplicaConfigReply;
import org.apache.bifromq.basekv.store.proto.ChangeReplicaConfigRequest;
import org.apache.bifromq.basekv.store.proto.KVRangeMergeReply;
import org.apache.bifromq.basekv.store.proto.KVRangeMergeRequest;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.KVRangeRWReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.store.proto.KVRangeSplitReply;
import org.apache.bifromq.basekv.store.proto.KVRangeSplitRequest;
import org.apache.bifromq.basekv.store.proto.RecoverReply;
import org.apache.bifromq.basekv.store.proto.RecoverRequest;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.store.proto.TransferLeadershipReply;
import org.apache.bifromq.basekv.store.proto.TransferLeadershipRequest;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.logger.SiftLogger;
import com.google.common.collect.Sets;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.slf4j.Logger;

@Accessors(fluent = true)
@Setter
class BaseKVStoreService extends BaseKVStoreServiceGrpc.BaseKVStoreServiceImplBase {
    private final Logger log;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final IKVRangeStore kvRangeStore;
    private final IAgentHost agentHost;
    private final IBaseKVClusterMetadataManager metadataManager;
    private final String clusterId;

    BaseKVStoreService(BaseKVStoreServiceBuilder builder) {
        kvRangeStore = new KVRangeStore(
            builder.clusterId,
            builder.storeOptions,
            builder.coProcFactory,
            builder.queryExecutor,
            builder.tickerThreads,
            builder.bgTaskExecutor,
            builder.attributes);
        this.clusterId = builder.clusterId;
        this.agentHost = builder.agentHost;
        log = SiftLogger.getLogger(BaseKVStoreService.class, "clusterId", clusterId, "storeId", kvRangeStore.id());
        metadataManager = builder.serverBuilder.metaService.metadataManager(clusterId);
    }

    public String clusterId() {
        return clusterId;
    }

    public String storeId() {
        return kvRangeStore.id();
    }

    public void start() {
        log.debug("Starting BaseKVStore service");
        kvRangeStore.start(new AgentHostStoreMessenger(agentHost, clusterId, kvRangeStore.id()));
        kvRangeStore.bootstrap(KVRangeIdUtil.generate(), FULL_BOUNDARY);
        // sync store descriptor via crdt
        disposables.add(kvRangeStore.describe().subscribe(metadataManager::report));
        log.debug("BaseKVStore service started");
    }

    public void stop() {
        log.debug("Stopping BaseKVStore service");
        metadataManager.stopReport(kvRangeStore.id()).join();
        disposables.dispose();
        kvRangeStore.stop();
        log.debug("BaseKVStore service stopped");
    }

    @Override
    public void bootstrap(BootstrapRequest request, StreamObserver<BootstrapReply> responseObserver) {
        response(tenantId -> kvRangeStore.bootstrap(request.getKvRangeId(), request.getBoundary())
            .handle((success, e) -> {
                if (e != null) {
                    log.error("Failed to bootstrap KVRange: rangeId={}, boundary={}",
                        KVRangeIdUtil.toString(request.getKvRangeId()), request.getBoundary(), e);
                    return BootstrapReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(BootstrapReply.Result.Error)
                        .build();
                }
                if (success) {
                    return BootstrapReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(BootstrapReply.Result.Ok)
                        .build();
                } else {
                    return BootstrapReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(BootstrapReply.Result.Exists)
                        .build();
                }
            }), responseObserver);
    }

    @Override
    public void recover(RecoverRequest request, StreamObserver<RecoverReply> responseObserver) {
        response(tenantId -> kvRangeStore.recover(request.getKvRangeId())
            .thenApply(result -> RecoverReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(RecoverReply.Result.Ok)
                .build())
            .exceptionally(e -> RecoverReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(e instanceof KVRangeStoreException
                    ? RecoverReply.Result.NotFound : RecoverReply.Result.Error)
                .build())
            .handle((v, e) -> RecoverReply.newBuilder().setReqId(request.getReqId()).build()), responseObserver);
    }

    @Override
    public void transferLeadership(TransferLeadershipRequest request,
                                   StreamObserver<TransferLeadershipReply> responseObserver) {
        response(tenantId -> kvRangeStore.transferLeadership(request.getVer(), request.getKvRangeId(),
                request.getNewLeaderStore())
            .thenApply(result -> TransferLeadershipReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .build())
            .exceptionally(unwrap(e -> {
                TransferLeadershipReply.Builder replyBuilder = TransferLeadershipReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(convertKVRangeException(e));
                if (e instanceof KVRangeException.BadVersion badVersion) {
                    if (badVersion.latest != null) {
                        replyBuilder.setLatest(badVersion.latest);
                    }
                }
                return replyBuilder.build();
            })), responseObserver);
    }

    @Override
    public void changeReplicaConfig(ChangeReplicaConfigRequest request,
                                    StreamObserver<ChangeReplicaConfigReply> responseObserver) {
        response(tenantId -> kvRangeStore.changeReplicaConfig(request.getVer(), request.getKvRangeId(),
                Sets.newHashSet(request.getNewVotersList()),
                Sets.newHashSet(request.getNewLearnersList()))
            .thenApply(result -> ChangeReplicaConfigReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .build())
            .exceptionally(unwrap(e -> {
                ChangeReplicaConfigReply.Builder replyBuilder = ChangeReplicaConfigReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(convertKVRangeException(e));
                if (e instanceof KVRangeException.BadVersion badVersion) {
                    if (badVersion.latest != null) {
                        replyBuilder.setLatest(badVersion.latest);
                    }
                }
                return replyBuilder.build();
            })), responseObserver);
    }

    @Override
    public void split(KVRangeSplitRequest request, StreamObserver<KVRangeSplitReply> responseObserver) {
        response(tenantId -> kvRangeStore.split(request.getVer(), request.getKvRangeId(), request.getSplitKey())
            .thenApply(result -> KVRangeSplitReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .build())
            .exceptionally(unwrap(e -> {
                KVRangeSplitReply.Builder replyBuilder = KVRangeSplitReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(convertKVRangeException(e));
                if (e instanceof KVRangeException.BadVersion badVersion) {
                    if (badVersion.latest != null) {
                        replyBuilder.setLatest(badVersion.latest);
                    }
                }
                return replyBuilder.build();
            })), responseObserver);
    }

    @Override
    public void merge(KVRangeMergeRequest request, StreamObserver<KVRangeMergeReply> responseObserver) {
        response(tenantId -> kvRangeStore.merge(request.getVer(), request.getMergerId(), request.getMergeeId())
            .thenApply(result -> KVRangeMergeReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .build())
            .exceptionally(unwrap(e -> {
                KVRangeMergeReply.Builder replyBuilder = KVRangeMergeReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(convertKVRangeException(e));
                if (e instanceof KVRangeException.BadVersion badVersion) {
                    if (badVersion.latest != null) {
                        replyBuilder.setLatest(badVersion.latest);
                    }
                }
                return replyBuilder.build();
            })), responseObserver);
    }

    @Override
    public StreamObserver<KVRangeRWRequest> execute(StreamObserver<KVRangeRWReply> responseObserver) {
        return new MutatePipeline(kvRangeStore, responseObserver);
    }

    @Override
    public StreamObserver<KVRangeRORequest> query(StreamObserver<KVRangeROReply> responseObserver) {
        return new QueryPipeline(kvRangeStore, false, responseObserver);
    }

    @Override
    public StreamObserver<KVRangeRORequest> linearizedQuery(StreamObserver<KVRangeROReply> responseObserver) {
        return new QueryPipeline(kvRangeStore, true, responseObserver);
    }

    private ReplyCode convertKVRangeException(Throwable e) {
        if (e instanceof KVRangeException.BadVersion) {
            return ReplyCode.BadVersion;
        }
        if (e instanceof KVRangeException.TryLater) {
            return ReplyCode.TryLater;
        }
        if (e instanceof KVRangeException.BadRequest) {
            return ReplyCode.BadRequest;
        }
        log.error("Internal Error", e);
        return ReplyCode.InternalError;
    }
}
