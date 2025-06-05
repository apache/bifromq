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

package org.apache.bifromq.retain.store.gc;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.client.exception.BadRequestException;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.InternalErrorException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.retain.rpc.proto.GCReply;
import org.apache.bifromq.retain.rpc.proto.GCRequest;
import org.apache.bifromq.retain.rpc.proto.RetainServiceRWCoProcInput;

@Slf4j
public class RetainStoreGCProcessor implements IRetainStoreGCProcessor {
    private final IBaseKVStoreClient storeClient;
    private final String localServerId;

    public RetainStoreGCProcessor(IBaseKVStoreClient storeClient, String localServerId) {
        this.storeClient = storeClient;
        this.localServerId = localServerId;
    }

    @Override
    public CompletableFuture<Result> gc(long reqId, String tenantId, Integer expirySeconds, long now) {
        Boundary boundary;
        if (tenantId == null) {
            boundary = FULL_BOUNDARY;
        } else {
            boundary = toBoundary(tenantBeginKey(tenantId), upperBound(tenantBeginKey(tenantId)));
        }
        CompletableFuture<?>[] gcFutures = findByBoundary(boundary, storeClient.latestEffectiveRouter())
            .stream()
            .filter(k -> localServerId == null || k.leader.equals(localServerId))
            .map(rangeSetting -> gcRange(reqId, rangeSetting, tenantId, expirySeconds, now))
            .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(gcFutures)
            .thenApply(v -> Arrays.stream(gcFutures).map(CompletableFuture::join).toList())
            .thenApply(v -> {
                log.debug("All range gc succeed");
                return Result.OK;
            })
            .exceptionally(unwrap(e -> {
                if (e instanceof ServerNotFoundException) {
                    return Result.TRY_LATER;
                }
                if (e instanceof TryLaterException) {
                    return Result.TRY_LATER;
                }
                if (e instanceof BadVersionException) {
                    return Result.TRY_LATER;
                }
                log.error("Some range gc failed", e);
                return Result.ERROR;
            }));
    }

    private CompletableFuture<GCReply> gcRange(long reqId,
                                               KVRangeSetting rangeSetting,
                                               String tenantId,
                                               Integer expirySeconds,
                                               long now) {
        GCRequest.Builder reqBuilder = GCRequest.newBuilder().setReqId(reqId).setNow(now);
        if (tenantId != null) {
            reqBuilder.setTenantId(tenantId);
        }
        if (expirySeconds != null) {
            reqBuilder.setExpirySeconds(expirySeconds);
        }
        return storeClient.execute(rangeSetting.leader, KVRangeRWRequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(rangeSetting.id)
                .setVer(rangeSetting.ver)
                .setRwCoProc(RWCoProcInput.newBuilder()
                    .setRetainService(RetainServiceRWCoProcInput.newBuilder()
                        .setGc(reqBuilder.build())
                        .build())
                    .build())
                .build())
            .thenApply(reply -> {
                switch (reply.getCode()) {
                    case Ok -> {
                        log.debug("Range gc succeed: serverId={}, rangeId={}, ver={}",
                            rangeSetting.leader, rangeSetting.id, rangeSetting.ver);
                        return reply.getRwCoProcResult().getRetainService().getGc();
                    }
                    case TryLater -> throw new TryLaterException();
                    case BadVersion -> throw new BadVersionException();
                    case BadRequest -> throw new BadRequestException();
                    default -> throw new InternalErrorException();
                }
            });
    }
}
