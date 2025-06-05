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

package org.apache.bifromq.sessiondict.client;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.sessiondict.SessionRegisterKeyUtil;
import org.apache.bifromq.sessiondict.client.scheduler.IOnlineCheckScheduler;
import org.apache.bifromq.sessiondict.client.scheduler.OnlineCheckScheduler;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckRequest;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.sessiondict.rpc.proto.GetReply;
import org.apache.bifromq.sessiondict.rpc.proto.GetRequest;
import org.apache.bifromq.sessiondict.rpc.proto.KillAllReply;
import org.apache.bifromq.sessiondict.rpc.proto.KillAllRequest;
import org.apache.bifromq.sessiondict.rpc.proto.KillReply;
import org.apache.bifromq.sessiondict.rpc.proto.KillRequest;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import org.apache.bifromq.sessiondict.rpc.proto.SubReply;
import org.apache.bifromq.sessiondict.rpc.proto.SubRequest;
import org.apache.bifromq.sessiondict.rpc.proto.UnsubReply;
import org.apache.bifromq.sessiondict.rpc.proto.UnsubRequest;
import org.apache.bifromq.type.ClientInfo;

@Slf4j
final class SessionDictClient implements ISessionDictClient {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final IRPCClient rpcClient;
    private final IOnlineCheckScheduler sessionExistScheduler;
    private final LoadingCache<ManagerCacheKey, SessionRegister> tenantSessionRegisterManagers;

    SessionDictClient(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
        sessionExistScheduler = new OnlineCheckScheduler(rpcClient);
        tenantSessionRegisterManagers = Caffeine.newBuilder()
            .weakValues()
            .executor(MoreExecutors.directExecutor())
            .build(key -> new SessionRegister(key.tenantId, key.registerKey, rpcClient));
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public ISessionRegistration reg(ClientInfo owner, IKillListener killListener) {
        return new SessionRegistration(owner, killListener,
            tenantSessionRegisterManagers.get(
                new ManagerCacheKey(owner.getTenantId(), SessionRegisterKeyUtil.toRegisterKey(owner))));
    }

    @Override
    public CompletableFuture<KillReply> kill(long reqId,
                                             String tenantId,
                                             String userId,
                                             String clientId,
                                             ClientInfo killer,
                                             ServerRedirection redirection) {
        return rpcClient.invoke(tenantId, null, KillRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setUserId(userId)
                .setClientId(clientId)
                .setKiller(killer)
                .setServerRedirection(redirection)
                .build(), SessionDictServiceGrpc.getKillMethod())
            .exceptionally(e -> KillReply.newBuilder()
                .setReqId(reqId)
                .setResult(KillReply.Result.ERROR)
                .build());
    }

    @Override
    public CompletableFuture<KillAllReply> killAll(long reqId,
                                                   String tenantId,
                                                   String userId, // nullable
                                                   ClientInfo killer,
                                                   ServerRedirection redirection) {
        KillAllRequest.Builder reqBuilder = KillAllRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setKiller(killer)
            .setServerRedirection(redirection);
        if (!Strings.isNullOrEmpty(userId)) {
            reqBuilder.setUserId(userId);
        }
        return (CompletableFuture<KillAllReply>) rpcClient.serverList()
            .firstElement()
            .map(Map::keySet)
            .toCompletionStage()
            .thenApply(servers ->
                servers.stream().map(serverId -> rpcClient.invoke(tenantId, serverId, reqBuilder.build(),
                    SessionDictServiceGrpc.getKillAllMethod())).toList())
            .thenCompose(killAllFutures -> CompletableFuture.allOf(killAllFutures.toArray(CompletableFuture[]::new))
                .thenApply(v -> killAllFutures.stream().map(CompletableFuture::join).toList()))
            .thenApply(killAllReplies -> {
                if (killAllReplies.stream()
                    .allMatch(reply -> reply.getResult() == KillAllReply.Result.OK)) {
                    return KillAllReply.newBuilder()
                        .setReqId(reqId)
                        .setResult(KillAllReply.Result.OK)
                        .build();
                } else {
                    return KillAllReply.newBuilder()
                        .setReqId(reqId)
                        .setResult(KillAllReply.Result.ERROR)
                        .build();
                }
            })
            .exceptionally(e -> {
                log.debug("Kill all failed", e);
                return KillAllReply.newBuilder()
                    .setReqId(reqId)
                    .setResult(KillAllReply.Result.ERROR)
                    .build();
            });

    }

    @Override
    public CompletableFuture<GetReply> get(GetRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request,
                SessionDictServiceGrpc.getGetMethod())
            .exceptionally(e -> {
                log.debug("Get failed", e);
                return GetReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(GetReply.Result.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<OnlineCheckResult> exist(OnlineCheckRequest clientId) {
        return sessionExistScheduler.schedule(clientId);
    }

    @Override
    public CompletableFuture<SubReply> sub(SubRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request,
                SessionDictServiceGrpc.getSubMethod())
            .exceptionally(e -> {
                log.debug("Sub failed", e);
                return SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(SubReply.Result.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<UnsubReply> unsub(UnsubRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request,
                SessionDictServiceGrpc.getUnsubMethod())
            .exceptionally(e -> {
                log.debug("Unsub failed", e);
                return UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(UnsubReply.Result.ERROR)
                    .build();
            });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.debug("Stopping session dict client");
            tenantSessionRegisterManagers.asMap().forEach((k, v) -> v.close());
            tenantSessionRegisterManagers.invalidateAll();
            sessionExistScheduler.close();
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.debug("Session dict client stopped");
        }
    }

    private record ManagerCacheKey(String tenantId, String registerKey) {
    }
}
