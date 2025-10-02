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
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
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
    public final CompletableFuture<Void> gc(long reqId, long now) {
        Collection<KVRangeSetting> rangeSettingList = Sets.newHashSet(findByBoundary(FULL_BOUNDARY,
            storeClient.latestEffectiveRouter()));
        if (rangeSettingList.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<?>[] gcResults = rangeSettingList.stream()
            .filter(setting -> setting.leader().equals(localServerId))
            .map(setting -> doGC(reqId, setting, now))
            .toArray(CompletableFuture[]::new);
        log.debug("[InboxStore] start gc: reqId={}, rangeCount={}", reqId, gcResults.length);
        return CompletableFuture.allOf(gcResults)
            .whenComplete((v, e) -> {
                if (e != null) {
                    log.debug("[InboxStore] gc failed: reqId={}", reqId, e);
                } else {
                    log.debug("[InboxStore] gc finished: reqId={}", reqId);
                }
            });
    }

    private CompletableFuture<Void> doGC(long reqId, KVRangeSetting rangeSetting, long now) {
        log.debug("[InboxStore] gc running: reqId={}, rangeId={}", reqId, KVRangeIdUtil.toString(rangeSetting.id()));
        return storeClient.query(rangeSetting.leader(), KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setKvRangeId(rangeSetting.id())
                .setVer(rangeSetting.ver())
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setInboxService(buildGCRequest(reqId, now))
                    .build())
                .build())
            .handle((v, e) -> {
                if (e != null) {
                    log.debug("[InboxStore] gc error: reqId={}, rangeId={}",
                        reqId, KVRangeIdUtil.toString(rangeSetting.id()), e);
                    return null;
                }
                if (v.getCode() == ReplyCode.Ok) {
                    log.debug("[InboxStore] gc done: reqId={}, rangeId={}",
                        reqId, KVRangeIdUtil.toString(rangeSetting.id()));
                } else {
                    log.debug("[InboxStore] gc rejected: reqId={}, rangeId={}, reason={}",
                        reqId, KVRangeIdUtil.toString(rangeSetting.id()), v.getCode());
                }
                return null;
            });
    }

    private InboxServiceROCoProcInput buildGCRequest(long reqId, long now) {
        return InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setGc(GCRequest.newBuilder().setNow(now).build())
            .build();
    }
}
