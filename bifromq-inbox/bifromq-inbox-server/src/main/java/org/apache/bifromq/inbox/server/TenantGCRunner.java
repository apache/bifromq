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
import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.tenantBeginKeyPrefix;

import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.client.exception.BadRequestException;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.InternalErrorException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllReply;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllRequest;
import org.apache.bifromq.inbox.storage.proto.ExpireTenantReply;
import org.apache.bifromq.inbox.storage.proto.ExpireTenantRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;

@Slf4j
public class TenantGCRunner implements ITenantGCRunner {
    private final IBaseKVStoreClient storeClient;

    public TenantGCRunner(IBaseKVStoreClient storeClient) {
        this.storeClient = storeClient;
    }

    public CompletableFuture<ExpireAllReply> expire(ExpireAllRequest request) {
        ByteString tenantBeginKey = tenantBeginKeyPrefix(request.getTenantId());
        Collection<KVRangeSetting> rangeSettingList = findByBoundary(
            toBoundary(tenantBeginKey, upperBound(tenantBeginKey)), storeClient.latestEffectiveRouter());
        if (rangeSettingList.isEmpty()) {
            return CompletableFuture.completedFuture(ExpireAllReply.newBuilder()
                .setReqId(request.getReqId())
                .setCode(ExpireAllReply.Code.OK)
                .build());
        }
        CompletableFuture<?>[] gcResults = rangeSettingList.stream()
            .map(setting -> expire(setting, request.getTenantId(), request.getExpirySeconds(), request.getNow()))
            .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(gcResults)
            .handle(unwrap((v, e) -> {
                if (e != null) {
                    if (e instanceof BadVersionException
                        || e instanceof TryLaterException
                        || e instanceof ServerNotFoundException) {
                        return ExpireAllReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setCode(ExpireAllReply.Code.TRY_LATER)
                            .build();
                    } else {
                        log.debug("[InboxGC] Failed to do gc: reqId={}", request.getReqId(), e);
                        return ExpireAllReply.newBuilder()
                            .setReqId(request.getReqId())
                            .setCode(ExpireAllReply.Code.ERROR)
                            .build();
                    }
                }
                return ExpireAllReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setCode(ExpireAllReply.Code.OK)
                    .build();
            }));
    }

    private CompletableFuture<ExpireTenantReply> expire(KVRangeSetting rangeSetting,
                                                        String tenantId,
                                                        Integer expirySeconds,
                                                        long now) {
        long reqId = System.nanoTime();
        return storeClient.query(rangeSetting.leader(), KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(rangeSetting.id())
                .setVer(rangeSetting.ver())
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setInboxService(buildRequest(reqId, tenantId, expirySeconds, now))
                    .build())
                .build())
            .thenApply(v -> {
                switch (v.getCode()) {
                    case Ok -> {
                        return v.getRoCoProcResult().getInboxService().getExpireTenant();
                    }
                    case BadRequest -> throw new BadRequestException();
                    case BadVersion -> throw new BadVersionException();
                    case TryLater -> throw new TryLaterException();
                    default -> throw new InternalErrorException();
                }
            });
    }

    private InboxServiceROCoProcInput buildRequest(long reqId, String tenantId, Integer expirySeconds, long now) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setExpireTenant(ExpireTenantRequest.newBuilder()
                .setNow(now)
                .setTenantId(tenantId)
                .setExpirySeconds(expirySeconds)
                .build())
            .build();
    }
}
