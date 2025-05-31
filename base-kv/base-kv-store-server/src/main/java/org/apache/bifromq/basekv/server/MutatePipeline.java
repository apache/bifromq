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

import org.apache.bifromq.basekv.raft.exception.DropProposalException;
import org.apache.bifromq.basekv.store.IKVRangeStore;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import org.apache.bifromq.basekv.store.proto.KVRangeRWReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.baserpc.server.ResponsePipeline;
import org.apache.bifromq.logger.SiftLogger;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;

class MutatePipeline extends ResponsePipeline<KVRangeRWRequest, KVRangeRWReply> {
    private final Logger log;
    private final IKVRangeStore kvRangeStore;

    MutatePipeline(IKVRangeStore kvRangeStore, StreamObserver<KVRangeRWReply> responseObserver) {
        super(responseObserver);
        this.kvRangeStore = kvRangeStore;
        this.log = SiftLogger.getLogger(MutatePipeline.class, "clusterId", kvRangeStore.clusterId(), "storeId",
            kvRangeStore.id());
    }

    @Override
    protected CompletableFuture<KVRangeRWReply> handleRequest(String s, KVRangeRWRequest request) {
        log.trace("Handling rw range request:req={}", request);
        return switch (request.getRequestTypeCase()) {
            case DELETE -> mutate(request, this::delete).toCompletableFuture();
            case PUT -> mutate(request, this::put).toCompletableFuture();
            default -> mutate(request, this::mutateCoProc).toCompletableFuture();
        };
    }

    private CompletionStage<KVRangeRWReply> delete(KVRangeRWRequest request) {
        return kvRangeStore.delete(request.getVer(), request.getKvRangeId(), request.getDelete())
            .thenApply(v -> KVRangeRWReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .setDeleteResult(v)
                .build());
    }

    private CompletionStage<KVRangeRWReply> put(KVRangeRWRequest request) {
        return kvRangeStore.put(request.getVer(), request.getKvRangeId(), request.getPut().getKey(),
                request.getPut().getValue())
            .thenApply(v -> KVRangeRWReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .setPutResult(v)
                .build());
    }

    private CompletionStage<KVRangeRWReply> mutateCoProc(KVRangeRWRequest request) {
        return kvRangeStore.mutateCoProc(request.getVer(), request.getKvRangeId(), request.getRwCoProc())
            .thenApply(v -> KVRangeRWReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .setRwCoProcResult(v)
                .build());
    }


    private CompletionStage<KVRangeRWReply> mutate(KVRangeRWRequest request, Function<KVRangeRWRequest,
        CompletionStage<KVRangeRWReply>> mutateFn) {
        return mutateFn.apply(request)
            .exceptionally(unwrap(e -> {
                if (e instanceof KVRangeException.BadVersion badVersion) {
                    KVRangeRWReply.Builder replyBuilder = KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.BadVersion);
                    if (badVersion.latest != null) {
                        replyBuilder.setLatest(badVersion.latest);
                    }
                    return replyBuilder.build();
                }
                if (e instanceof KVRangeStoreException.KVRangeNotFoundException
                    || e instanceof KVRangeException.TryLater) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.TryLater)
                        .build();
                }
                if (e instanceof KVRangeException.BadRequest) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.BadRequest)
                        .build();
                }
                if (e instanceof DropProposalException.TransferringLeaderException) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.TryLater)
                        .build();
                }
                if (e instanceof DropProposalException.NoLeaderException) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.TryLater)
                        .build();
                }
                if (e instanceof DropProposalException.ForwardTimeoutException) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.TryLater)
                        .build();
                }
                if (e instanceof DropProposalException.OverriddenException) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.TryLater)
                        .build();
                }
                if (e instanceof DropProposalException.SupersededBySnapshotException) {
                    return KVRangeRWReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ReplyCode.TryLater)
                        .build();
                }
                log.debug("Handle rw request error: reqId={}", request.getReqId(), e);
                return KVRangeRWReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ReplyCode.InternalError)
                    .build();
            }));
    }
}
