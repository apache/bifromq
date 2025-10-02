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

package org.apache.bifromq.inbox.client;

import static org.apache.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baserpc.client.IRPCClient;
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
import org.apache.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import org.apache.bifromq.inbox.rpc.proto.InboxStateReply;
import org.apache.bifromq.inbox.rpc.proto.InboxStateRequest;
import org.apache.bifromq.inbox.rpc.proto.SendLWTReply;
import org.apache.bifromq.inbox.rpc.proto.SendLWTRequest;
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.inbox.rpc.proto.UnsubRequest;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.plugin.subbroker.IDeliverer;
import org.apache.bifromq.type.MatchInfo;

@Slf4j
final class InboxClient implements IInboxClient {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final IRPCClient rpcClient;
    private final LoadingCache<FetchPipelineKey, InboxFetchPipeline> fetchPipelineCache;

    InboxClient(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
        fetchPipelineCache = Caffeine.newBuilder()
            .weakValues()
            .executor(MoreExecutors.directExecutor())
            .build(key -> new InboxFetchPipeline(key.tenantId, key.delivererKey, rpcClient));
    }

    @Override
    public IDeliverer open(String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return new InboxDeliverPipeline(delivererKey, rpcClient);
    }

    @Override
    public Observable<ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public IInboxReader openInboxReader(String tenantId, String inboxId, long incarnation) {
        return new InboxReader(inboxId, incarnation,
            fetchPipelineCache.get(new FetchPipelineKey(tenantId, getDelivererKey(tenantId, inboxId))));
    }

    @Override
    public CompletableFuture<CheckReply> check(CheckRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getCheckSubscriptionsMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle CheckRequest", e);
                CheckReply.Builder replyBuilder = CheckReply.newBuilder();
                for (MatchInfo matchInfo : request.getMatchInfoList()) {
                    replyBuilder.addCode(CheckReply.Code.ERROR);
                }
                return replyBuilder.build();
            });
    }

    @Override
    public CompletableFuture<CommitReply> commit(CommitRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getCommitMethod())
            .exceptionally(e -> {
                log.debug("Failed to commit inbox", e);
                return CommitReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(CommitReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<InboxStateReply> state(long reqId, String tenantId, String userId, String clientId) {
        return rpcClient.invoke(tenantId, null, InboxStateRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setInboxId(userId + "/" + clientId)
                .setNow(System.currentTimeMillis())
                .build(), InboxServiceGrpc.getStateMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle InboxStateRequest", e);
                return InboxStateReply.newBuilder()
                    .setReqId(reqId)
                    .setCode(InboxStateReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<ExistReply> exist(ExistRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getExistMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle ExistRequest", e);
                return ExistReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ExistReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<AttachReply> attach(AttachRequest request) {
        return rpcClient.invoke(request.getClient().getTenantId(), null, request, InboxServiceGrpc.getAttachMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle AttachRequest", e);
                return AttachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(AttachReply.Code.ERROR).build();
            });
    }

    @Override
    public CompletableFuture<DetachReply> detach(DetachRequest request) {
        return rpcClient.invoke(request.getClient().getTenantId(), null, request, InboxServiceGrpc.getDetachMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle DetachRequest", e);
                return DetachReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(DetachReply.Code.ERROR).build();
            });
    }

    @Override
    public CompletableFuture<SubReply> sub(SubRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getSubMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle SubRequest", e);
                return SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(SubReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<UnsubReply> unsub(UnsubRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getUnsubMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle UnsubRequest", e);
                return UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(UnsubReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<SendLWTReply> sendLWT(SendLWTRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getSendLWTMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle SendLWTRequest", e);
                return SendLWTReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(SendLWTReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<DeleteReply> delete(DeleteRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getDeleteMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle DeleteRequest", e);
                return DeleteReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(DeleteReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<ExpireAllReply> expireAll(ExpireAllRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, InboxServiceGrpc.getExpireAllMethod())
            .exceptionally(e -> {
                log.debug("Failed to handle ExpireAllRequest", e);
                return ExpireAllReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ExpireAllReply.Code.ERROR)
                    .build();
            });
    }

    @Override
    public void close() {
        if (hasStopped.compareAndSet(false, true)) {
            log.debug("Closing inbox client");
            fetchPipelineCache.asMap().forEach((k, v) -> v.close());
            fetchPipelineCache.invalidateAll();
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.debug("Inbox client closed");
        }
    }

    private record FetchPipelineKey(String tenantId, String delivererKey) {
    }
}
