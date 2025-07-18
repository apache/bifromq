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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;

import com.google.common.collect.Sets;
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
import org.apache.bifromq.inbox.storage.proto.GCReply;
import org.apache.bifromq.inbox.storage.proto.GCRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;

@Slf4j
public class InboxStoreGCProcessor implements IInboxStoreGCProcessor {
    protected final IBaseKVStoreClient storeClient;
    private final String localServerId;

    public InboxStoreGCProcessor(IBaseKVStoreClient storeClient, String localStoreId) {
        this.storeClient = storeClient;
        this.localServerId = localStoreId;
    }

    @Override
    public final CompletableFuture<Result> gc(long reqId, long now) {
        Collection<KVRangeSetting> rangeSettingList = Sets.newHashSet(findByBoundary(FULL_BOUNDARY,
            storeClient.latestEffectiveRouter()));
        if (localServerId != null) {
            rangeSettingList.removeIf(rangeSetting -> !rangeSetting.leader.equals(localServerId));
        }
        if (rangeSettingList.isEmpty()) {
            return CompletableFuture.completedFuture(Result.OK);
        }
        CompletableFuture<?>[] gcResults = rangeSettingList.stream().map(
            setting -> doGC(reqId, setting, now)).toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(gcResults)
            .handle((v, e) -> {
                if (e != null) {
                    log.debug("[InboxGC] Failed to do gc: reqId={}", reqId, e);
                    return Result.ERROR;
                }
                return Result.OK;
            });
    }

    private CompletableFuture<GCReply> doGC(long reqId, KVRangeSetting rangeSetting, long now) {
        return storeClient.query(rangeSetting.leader, KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(rangeSetting.id)
                .setVer(rangeSetting.ver)
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setInboxService(buildGCRequest(reqId, now))
                    .build())
                .build())
            .thenApply(v -> {
                switch (v.getCode()) {
                    case Ok -> {
                        return v.getRoCoProcResult().getInboxService().getGc();
                    }
                    case BadRequest -> throw new BadRequestException();
                    case BadVersion -> throw new BadVersionException();
                    case TryLater -> throw new TryLaterException();
                    default -> throw new InternalErrorException();
                }
            });
    }

    private InboxServiceROCoProcInput buildGCRequest(long reqId, long now) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setGc(GCRequest.newBuilder().setNow(now).build())
            .build();
    }
}
