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

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;
import static org.apache.bifromq.baserpc.server.UnaryResponse.response;
import static org.apache.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;
import static org.apache.bifromq.inbox.util.InboxServiceUtil.receiverId;

import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.basescheduler.exception.BatcherUnavailableException;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.AttachRequest;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.rpc.proto.DeleteReply;
import org.apache.bifromq.inbox.rpc.proto.DeleteRequest;
import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.inbox.rpc.proto.DetachRequest;
import org.apache.bifromq.inbox.rpc.proto.ExistReply;
import org.apache.bifromq.inbox.rpc.proto.ExistRequest;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllReply;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllRequest;
import org.apache.bifromq.inbox.rpc.proto.InboxFetchHint;
import org.apache.bifromq.inbox.rpc.proto.InboxFetched;
import org.apache.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import org.apache.bifromq.inbox.rpc.proto.InboxStateReply;
import org.apache.bifromq.inbox.rpc.proto.InboxStateRequest;
import org.apache.bifromq.inbox.rpc.proto.SendLWTReply;
import org.apache.bifromq.inbox.rpc.proto.SendLWTRequest;
import org.apache.bifromq.inbox.rpc.proto.SendReply;
import org.apache.bifromq.inbox.rpc.proto.SendRequest;
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.inbox.rpc.proto.UnsubRequest;
import org.apache.bifromq.inbox.server.scheduler.CheckMatchInfo;
import org.apache.bifromq.inbox.server.scheduler.IInboxAttachScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxCheckSubScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxCommitScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxDeleteScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxDetachScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxExistScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxFetchScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxFetchStateScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxSendLWTScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxSubScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxUnsubScheduler;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.util.TopicUtil;

@Slf4j
class InboxService extends InboxServiceGrpc.InboxServiceImplBase {
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final IInboxClient inboxClient;
    private final IDistClient distClient;
    private final InboxFetcherRegistry registry = new InboxFetcherRegistry();
    private final IInboxFetchScheduler fetchScheduler;
    private final IInboxFetchStateScheduler fetchStateScheduler;
    private final IInboxExistScheduler existScheduler;
    private final IInboxSendLWTScheduler sendLWTScheduler;
    private final IInboxCheckSubScheduler checkSubScheduler;
    private final IInboxInsertScheduler insertScheduler;
    private final IInboxCommitScheduler commitScheduler;
    private final IInboxAttachScheduler attachScheduler;
    private final IInboxDetachScheduler detachScheduler;
    private final IInboxDeleteScheduler deleteScheduler;
    private final IInboxSubScheduler subScheduler;
    private final IInboxUnsubScheduler unsubScheduler;
    private final ITenantGCRunner tenantGCRunner;

    @Builder
    InboxService(IInboxClient inboxClient,
                 IDistClient distClient,
                 IInboxFetchStateScheduler fetchStateScheduler,
                 IInboxExistScheduler existScheduler,
                 IInboxSendLWTScheduler sendLWTScheduler,
                 IInboxCheckSubScheduler checkSubScheduler,
                 IInboxFetchScheduler fetchScheduler,
                 IInboxInsertScheduler insertScheduler,
                 IInboxCommitScheduler commitScheduler,
                 IInboxAttachScheduler attachScheduler,
                 IInboxDetachScheduler detachScheduler,
                 IInboxDeleteScheduler deleteScheduler,
                 IInboxSubScheduler subScheduler,
                 IInboxUnsubScheduler unsubScheduler,
                 ITenantGCRunner tenantGCRunner) {
        this.inboxClient = inboxClient;
        this.distClient = distClient;
        this.fetchStateScheduler = fetchStateScheduler;
        this.existScheduler = existScheduler;
        this.sendLWTScheduler = sendLWTScheduler;
        this.checkSubScheduler = checkSubScheduler;
        this.fetchScheduler = fetchScheduler;
        this.insertScheduler = insertScheduler;
        this.commitScheduler = commitScheduler;
        this.attachScheduler = attachScheduler;
        this.detachScheduler = detachScheduler;
        this.deleteScheduler = deleteScheduler;
        this.subScheduler = subScheduler;
        this.unsubScheduler = unsubScheduler;
        this.tenantGCRunner = tenantGCRunner;
    }

    @Override
    public void state(InboxStateRequest request, StreamObserver<InboxStateReply> responseObserver) {
        log.trace("Handling InboxStateRequest: {}", request);
        response(tenantId -> fetchStateScheduler.schedule(request)
            .exceptionally(unwrap(e -> {
                if (e instanceof BatcherUnavailableException) {
                    return InboxStateReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(InboxStateReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException) {
                    return InboxStateReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(InboxStateReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to fetch inbox state", e);
                return InboxStateReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(InboxStateReply.Code.ERROR)
                    .build();
            })), responseObserver);
    }

    @Override
    public void exist(ExistRequest request, StreamObserver<ExistReply> responseObserver) {
        log.trace("Handling ExistRequest: {}", request);
        response(tenantId -> existScheduler.schedule(request)
            .exceptionally(unwrap(e -> {
                if (e instanceof BatcherUnavailableException) {
                    return ExistReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ExistReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException) {
                    return ExistReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(ExistReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to handle ExistRequest", e);
                return ExistReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ExistReply.Code.ERROR)
                    .build();
            })), responseObserver);
    }

    @Override
    public void attach(AttachRequest request, StreamObserver<AttachReply> responseObserver) {
        log.trace("Handling AttachRequest: {}", request);
        assert !request.hasLwt() || request.getLwt().getDelaySeconds() > 0;
        response(tenantId -> attachScheduler.schedule(request)
            .exceptionally(unwrap(e -> {
                if (e instanceof BatcherUnavailableException) {
                    return AttachReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(AttachReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException) {
                    return AttachReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(AttachReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to handle AttachRequest", e);
                return AttachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(AttachReply.Code.ERROR)
                    .build();
            })), responseObserver);
    }

    @Override
    public void detach(DetachRequest request, StreamObserver<DetachReply> responseObserver) {
        log.trace("Handling DetachRequest: {}", request);
        response(tenantId -> detachScheduler.schedule(request)
            .exceptionally(unwrap(e -> {
                if (e instanceof BatcherUnavailableException) {
                    return DetachReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(DetachReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException) {
                    return DetachReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(DetachReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to handle DetachRequest", e);
                return DetachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(DetachReply.Code.ERROR)
                    .build();
            })), responseObserver);
    }

    @Override
    public void sub(SubRequest request, StreamObserver<SubReply> responseObserver) {
        log.trace("Handling SubRequest: {}", request);
        response(tenantId -> subScheduler.schedule(request)
            .thenCompose(subReply -> {
                if (subReply.getCode() == SubReply.Code.OK || subReply.getCode() == SubReply.Code.EXISTS) {
                    return distClient.addRoute(request.getReqId(),
                            request.getTenantId(),
                            TopicUtil.from(request.getTopicFilter()),
                            receiverId(request.getInboxId(), request.getVersion().getIncarnation()),
                            getDelivererKey(request.getTenantId(), request.getInboxId()),
                            inboxClient.id(),
                            request.getOption().getIncarnation())
                        .thenApply(matchResult -> {
                            switch (matchResult) {
                                case OK -> {
                                    return subReply;
                                }
                                case EXCEED_LIMIT -> {
                                    return SubReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(SubReply.Code.EXCEED_LIMIT).build();
                                }
                                case BACK_PRESSURE_REJECTED -> {
                                    return SubReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(SubReply.Code.BACK_PRESSURE_REJECTED).build();
                                }
                                case TRY_LATER -> {
                                    return SubReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(SubReply.Code.TRY_LATER).build();
                                }
                                default -> {
                                    return SubReply.newBuilder()
                                        .setReqId(request.getReqId())
                                        .setCode(SubReply.Code.ERROR).build();
                                }
                            }
                        });
                }
                return CompletableFuture.completedFuture(subReply);
            })
            .exceptionally(unwrap(e -> {
                log.debug("Failed to handle SubRequest", e);
                if (e instanceof BatcherUnavailableException) {
                    return SubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(SubReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException) {
                    return SubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(SubReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                return SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(SubReply.Code.ERROR)
                    .build();
            })), responseObserver);
    }

    @Override
    public void unsub(UnsubRequest request, StreamObserver<UnsubReply> responseObserver) {
        log.trace("Handling UnsubRequest: {}", request);
        response(tenantId -> unsubScheduler.schedule(request)
            .thenCompose(v -> {
                if (v.getCode() == UnsubReply.Code.OK) {
                    return unmatch(request.getReqId(),
                        request.getTenantId(),
                        request.getInboxId(),
                        request.getVersion().getIncarnation(),
                        request.getTopicFilter(),
                        v.getOption())
                        .thenApply(unmatchResult -> switch (unmatchResult) {
                            case OK -> v;
                            case NOT_EXISTED -> UnsubReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(UnsubReply.Code.NO_SUB)
                                .build();
                            case BACK_PRESSURE_REJECTED -> UnsubReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(UnsubReply.Code.BACK_PRESSURE_REJECTED)
                                .build();
                            case TRY_LATER -> UnsubReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(UnsubReply.Code.TRY_LATER)
                                .build();
                            case ERROR -> UnsubReply.newBuilder()
                                .setReqId(request.getReqId())
                                .setCode(UnsubReply.Code.ERROR)
                                .build();
                        });
                }
                return CompletableFuture.completedFuture(v);
            })
            .exceptionally(unwrap(e -> {
                if (e instanceof BatcherUnavailableException) {
                    return UnsubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(UnsubReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException) {
                    return UnsubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(UnsubReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to handle UnsubRequest", e);
                return UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(UnsubReply.Code.ERROR)
                    .build();
            })), responseObserver);
    }

    private CompletableFuture<UnmatchResult> unmatch(long reqId,
                                                     String tenantId,
                                                     String inboxId,
                                                     long incarnation,
                                                     String topicFilter,
                                                     TopicFilterOption option) {
        return distClient.removeRoute(reqId, tenantId, TopicUtil.from(topicFilter), receiverId(inboxId, incarnation),
            getDelivererKey(tenantId, inboxId), inboxClient.id(), option.getIncarnation());
    }

    @Override
    public void expireAll(ExpireAllRequest request, StreamObserver<ExpireAllReply> responseObserver) {
        log.trace("Handling ExpireAllRequest: {}", request);
        response(tenantId -> tenantGCRunner.expire(request), responseObserver);
    }

    @Override
    public void checkSubscriptions(CheckRequest request, StreamObserver<CheckReply> responseObserver) {
        log.trace("Handling CheckRequest: {}", request);
        response(tenantId -> {
            List<CompletableFuture<CheckReply.Code>> futures = request.getMatchInfoList().stream()
                .map(matchInfo -> checkSubScheduler.schedule(new CheckMatchInfo(request.getTenantId(), matchInfo))
                    .exceptionally(unwrap(e -> {
                        if (e instanceof BatcherUnavailableException) {
                            return CheckReply.Code.TRY_LATER;
                        }
                        log.debug("Failed to handle CheckRequest", e);
                        return CheckReply.Code.ERROR;
                    })))
                .toList();
            return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).toList())
                .thenApply(codes -> CheckReply.newBuilder()
                    .addAllCode(codes)
                    .build());
        }, responseObserver);
    }

    @Override
    public StreamObserver<SendRequest> receive(StreamObserver<SendReply> responseObserver) {
        return new InboxWriterPipeline(new FetcherSignaler(registry),
            new InboxWriter(insertScheduler), responseObserver);
    }

    @Override
    public StreamObserver<InboxFetchHint> fetch(StreamObserver<InboxFetched> responseObserver) {
        return new InboxFetchPipeline(responseObserver, fetchScheduler::schedule, registry);
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<CommitReply> responseObserver) {
        log.trace("Handling CommitRequest: {}", request);
        response(tenantId -> commitScheduler.schedule(request)
            .exceptionally(unwrap(e -> {
                if (e instanceof BatcherUnavailableException) {
                    return CommitReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(CommitReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException) {
                    return CommitReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(CommitReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to handle CommitRequest", e);
                return CommitReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(CommitReply.Code.ERROR)
                    .build();

            })), responseObserver);
    }

    @Override
    public void sendLWT(SendLWTRequest request, StreamObserver<SendLWTReply> responseObserver) {
        log.trace("Handling SendLWTRequest: {}", request);
        response(tenantId -> sendLWTScheduler.schedule(request)
            .exceptionally(unwrap(e -> {
                if (e instanceof BatcherUnavailableException) {
                    return SendLWTReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(SendLWTReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException) {
                    return SendLWTReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(SendLWTReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to handle SendLWTRequest", e);
                return SendLWTReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(SendLWTReply.Code.ERROR)
                    .build();
            })), responseObserver);
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteReply> responseObserver) {
        log.trace("Handling DeleteRequest: {}", request);
        response(tenantId -> delete(request), responseObserver);
    }

    private CompletableFuture<DeleteReply> delete(DeleteRequest request) {
        return deleteScheduler.schedule(request)
            .thenCompose(result -> {
                if (result.getCode() == DeleteReply.Code.OK) {
                    List<CompletableFuture<UnmatchResult>> unmatchFutures =
                        result.getTopicFiltersMap().entrySet().stream()
                            .map(e -> unmatch(System.nanoTime(),
                                request.getTenantId(),
                                request.getInboxId(),
                                request.getVersion().getIncarnation(),
                                e.getKey(),
                                e.getValue()))
                            .toList();
                    return CompletableFuture.allOf(unmatchFutures.toArray(CompletableFuture[]::new))
                        .thenApply(v -> result);
                }
                return CompletableFuture.completedFuture(result);
            })
            .exceptionally(unwrap(e -> {
                if (e instanceof BatcherUnavailableException) {
                    return DeleteReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(DeleteReply.Code.TRY_LATER)
                        .build();
                }
                if (e instanceof BackPressureException) {
                    return DeleteReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setCode(DeleteReply.Code.BACK_PRESSURE_REJECTED)
                        .build();
                }
                log.debug("Failed to handle DeleteRequest", e);
                return DeleteReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(DeleteReply.Code.ERROR)
                    .build();
            }));
    }

    public void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            state.set(State.STARTED);
        }
    }

    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            for (IInboxFetcher fetcher : registry) {
                fetcher.close();
            }
            fetchStateScheduler.close();
            existScheduler.close();
            attachScheduler.close();
            detachScheduler.close();
            deleteScheduler.close();
            subScheduler.close();
            unsubScheduler.close();

            fetchScheduler.close();
            insertScheduler.close();
            commitScheduler.close();

            state.set(State.STOPPED);
        }
    }

    private enum State {
        INIT,
        STARTING,
        STARTED,
        STOPPING,
        STOPPED
    }
}
