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

package org.apache.bifromq.basekv.server;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.bifromq.basekv.raft.exception.ReadIndexException;
import org.apache.bifromq.basekv.store.IKVRangeStore;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.NullableValue;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.baserpc.server.ResponsePipeline;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class QueryPipeline extends ResponsePipeline<KVRangeRORequest, KVRangeROReply> {

    private final Logger log;
    private final ConcurrentLinkedQueue<QueryTask> requests = new ConcurrentLinkedQueue<>();
    private final IKVRangeStore kvRangeStore;
    private final boolean linearized;
    private final AtomicBoolean executing = new AtomicBoolean(false);

    public QueryPipeline(IKVRangeStore kvRangeStore, boolean linearized,
                         StreamObserver<KVRangeROReply> responseObserver) {
        super(responseObserver);
        this.linearized = linearized;
        this.kvRangeStore = kvRangeStore;
        this.log = MDCLogger.getLogger(QueryPipeline.class, "clusterId", kvRangeStore.clusterId(), "storeId",
            kvRangeStore.id());
    }

    @Override
    protected CompletableFuture<KVRangeROReply> handleRequest(String ignore, KVRangeRORequest request) {
        QueryTask task = switch (request.getTypeCase()) {
            case GETKEY -> new QueryTask(request, this::get);
            case EXISTKEY -> new QueryTask(request, this::exist);
            default -> new QueryTask(request, this::roCoproc);
        };
        log.trace("Submit ro request:\n{}", request);
        requests.add(task);
        submitForExecution();
        return task.onDone;
    }

    private void submitForExecution() {
        if (executing.compareAndSet(false, true)) {
            QueryTask task = requests.poll();
            if (task != null) {
                KVRangeRORequest request = task.request;
                if (task.onDone.isCancelled()) {
                    log.trace("Skip submit ro range request[linearized={}] to store:\n{}", linearized, request);
                }
                task.queryFn.apply(request)
                    .exceptionally(unwrap(e -> {
                        if (e instanceof KVRangeException.BadVersion badVersion) {
                            KVRangeROReply.Builder replyBuilder = KVRangeROReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(ReplyCode.BadVersion);
                            if (badVersion.latest != null) {
                                replyBuilder.setLatest(badVersion.latest);
                            }
                            return replyBuilder.build();
                        }
                        if (e instanceof KVRangeException.TryLater tryLater) {
                            KVRangeROReply.Builder replyBuilder = KVRangeROReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(ReplyCode.TryLater);
                            if (tryLater.latest != null) {
                                replyBuilder.setLatest(tryLater.latest);
                            }
                            return replyBuilder.build();
                        }
                        if (e instanceof KVRangeStoreException.KVRangeNotFoundException) {
                            return KVRangeROReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(ReplyCode.TryLater)
                                .build();
                        }
                        if (e instanceof KVRangeException.BadRequest badRequest) {
                            KVRangeROReply.Builder replyBuilder = KVRangeROReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(ReplyCode.BadRequest);
                            if (badRequest.latest != null) {
                                replyBuilder.setLatest(badRequest.latest);
                            }
                            return replyBuilder.build();
                        }
                        if (e instanceof ReadIndexException) {
                            return KVRangeROReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(ReplyCode.TryLater)
                                .build();
                        }
                        log.debug("query range error: reqId={}", request.getReqId(), e);
                        return KVRangeROReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setCode(ReplyCode.InternalError)
                            .build();
                    }))
                    .thenAccept(v -> {
                        task.onDone.complete(v);
                        executing.set(false);
                        if (!requests.isEmpty()) {
                            submitForExecution();
                        }
                    });
            } else {
                executing.set(false);
                if (!requests.isEmpty()) {
                    submitForExecution();
                }
            }
        }
    }

    private CompletionStage<KVRangeROReply> exist(KVRangeRORequest request) {
        return kvRangeStore.exist(request.getVer(), request.getKvRangeId(), request.getExistKey(), linearized)
            .thenApply(result -> KVRangeROReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .setExistResult(result)
                .build());
    }

    private CompletionStage<KVRangeROReply> get(KVRangeRORequest request) {
        return kvRangeStore.get(request.getVer(), request.getKvRangeId(), request.getGetKey(), linearized)
            .thenApply(result -> KVRangeROReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .setGetResult(result.map(v -> NullableValue.newBuilder().setValue(v).build())
                    .orElse(NullableValue.getDefaultInstance()))
                .build());
    }

    private CompletionStage<KVRangeROReply> roCoproc(KVRangeRORequest request) {
        return kvRangeStore.queryCoProc(request.getVer(), request.getKvRangeId(), request.getRoCoProc(), linearized)
            .thenApply(result -> KVRangeROReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ReplyCode.Ok)
                .setRoCoProcResult(result)
                .build());
    }

    @Override
    protected void afterClose() {
        requests.clear();
    }

    private static class QueryTask {
        final KVRangeRORequest request;
        final Function<KVRangeRORequest, CompletionStage<KVRangeROReply>> queryFn;
        final CompletableFuture<KVRangeROReply> onDone = new CompletableFuture<>();

        QueryTask(KVRangeRORequest request, Function<KVRangeRORequest, CompletionStage<KVRangeROReply>> queryFn) {
            this.request = request;
            this.queryFn = queryFn;
        }
    }
}
