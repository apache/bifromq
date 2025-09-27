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

package org.apache.bifromq.dist.client;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.dist.client.scheduler.BatchPubCallBuilderFactory;
import org.apache.bifromq.dist.client.scheduler.IPubCallScheduler;
import org.apache.bifromq.dist.client.scheduler.PubCallScheduler;
import org.apache.bifromq.dist.client.scheduler.PubRequest;
import org.apache.bifromq.dist.rpc.proto.DistServiceGrpc;
import org.apache.bifromq.dist.rpc.proto.MatchRequest;
import org.apache.bifromq.dist.rpc.proto.UnmatchRequest;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.RouteMatcher;

@Slf4j
final class DistClient implements IDistClient {
    private final IPubCallScheduler reqScheduler;
    private final IRPCClient rpcClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DistClient(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
        reqScheduler = new PubCallScheduler(new BatchPubCallBuilderFactory(rpcClient));
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public CompletableFuture<PubResult> pub(long reqId, String topic, Message message, ClientInfo publisher) {
        return reqScheduler.schedule(new PubRequest(publisher, topic, message))
            .exceptionally(unwrap(e -> {
                if (e instanceof BackPressureException) {
                    return PubResult.BACK_PRESSURE_REJECTED;
                }
                log.debug("Failed to pub", e);
                return PubResult.ERROR;
            }));
    }

    @Override
    public CompletableFuture<MatchResult> addRoute(long reqId,
                                                   String tenantId,
                                                   RouteMatcher matcher,
                                                   String receiverId,
                                                   String delivererKey,
                                                   int subBrokerId,
                                                   long incarnation) {
        MatchRequest request = MatchRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setMatcher(matcher)
            .setReceiverId(receiverId)
            .setDelivererKey(delivererKey)
            .setBrokerId(subBrokerId)
            .setIncarnation(incarnation)
            .build();
        log.trace("Handling match request:\n{}", request);
        return rpcClient.invoke(tenantId, null, request, DistServiceGrpc.getMatchMethod())
            .thenApply(v -> switch (v.getResult()) {
                case OK -> MatchResult.OK;
                case EXCEED_LIMIT -> MatchResult.EXCEED_LIMIT;
                case BACK_PRESSURE_REJECTED -> MatchResult.BACK_PRESSURE_REJECTED;
                case TRY_LATER -> MatchResult.TRY_LATER;
                default -> MatchResult.ERROR;
            })
            .exceptionally(e -> {
                log.debug("Failed to match", e);
                return MatchResult.ERROR;
            });
    }

    @Override
    public CompletableFuture<UnmatchResult> removeRoute(long reqId,
                                                        String tenantId,
                                                        RouteMatcher matcher,
                                                        String receiverId,
                                                        String delivererKey,
                                                        int subBrokerId,
                                                        long incarnation) {
        UnmatchRequest request = UnmatchRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setMatcher(matcher)
            .setReceiverId(receiverId)
            .setDelivererKey(delivererKey)
            .setBrokerId(subBrokerId)
            .setIncarnation(incarnation)
            .build();
        log.trace("Handling unsub request:\n{}", request);
        return rpcClient.invoke(tenantId, null, request, DistServiceGrpc.getUnmatchMethod())
            .thenApply(v -> switch (v.getResult()) {
                case OK -> UnmatchResult.OK;
                case NOT_EXISTED -> UnmatchResult.NOT_EXISTED;
                case BACK_PRESSURE_REJECTED -> UnmatchResult.BACK_PRESSURE_REJECTED;
                case TRY_LATER -> UnmatchResult.TRY_LATER;
                default -> UnmatchResult.ERROR;
            })
            .exceptionally(e -> {
                log.debug("Failed to unmatch", e);
                return UnmatchResult.ERROR;
            });
    }

    @Override
    public void close() {
        // close tenant logger and drain logs before closing the dist client
        if (closed.compareAndSet(false, true)) {
            log.debug("Stopping dist client");
            log.debug("Closing request scheduler");
            reqScheduler.close();
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.debug("Dist client stopped");
        }
    }
}
