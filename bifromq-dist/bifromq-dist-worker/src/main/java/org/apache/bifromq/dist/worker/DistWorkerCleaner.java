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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;

import com.google.protobuf.ByteString;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.GCReply;
import org.apache.bifromq.dist.rpc.proto.GCRequest;

@Slf4j
class DistWorkerCleaner {
    private static final int MAX_STEP = 10;
    private static final int SCAN_QUOTA_BASE = 512;
    private static final double HIGH_SUCCESS_RATIO = 0.005; // 0.5%
    private static final int LOW_HIT_THRESHOLD = 3;
    private static final double QUOTA_HIT_RATIO = 0.8;
    private static final double GROWTH = 1.5;
    private static final double SHRINK = 2.0;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final IBaseKVStoreClient distWorkerClient;
    private final Duration minCleanInterval;
    private final Duration maxCleanInterval;
    private final ScheduledExecutorService jobScheduler;
    private final Clock clock;
    // rangeId -> state
    private final Map<KVRangeId, RangeGcState> rangeStates = new HashMap<>();
    private volatile ScheduledFuture<?> cleanerFuture;

    DistWorkerCleaner(IBaseKVStoreClient distWorkerClient,
                      Duration minCleanInterval,
                      Duration maxCleanInterval,
                      ScheduledExecutorService jobScheduler) {
        this(distWorkerClient, minCleanInterval, maxCleanInterval, jobScheduler, Clock.systemUTC());
    }

    DistWorkerCleaner(IBaseKVStoreClient distWorkerClient,
                      Duration minCleanInterval,
                      Duration maxCleanInterval,
                      ScheduledExecutorService jobScheduler,
                      Clock clock) {
        this.distWorkerClient = distWorkerClient;
        this.minCleanInterval = minCleanInterval;
        this.maxCleanInterval = maxCleanInterval;
        this.jobScheduler = jobScheduler;
        this.clock = clock;
    }

    void start(String storeId) {
        if (started.compareAndSet(false, true)) {
            log.debug("DistWorkerCleaner started");
            doStart(storeId);
        }
    }

    CompletableFuture<Void> stop() {
        if (started.compareAndSet(true, false)) {
            cleanerFuture.cancel(true);
            CompletableFuture<Void> onDone = new CompletableFuture<>();
            jobScheduler.execute(() -> {
                log.debug("DistWorkerCleaner stopped");
                onDone.complete(null);
            });
            return onDone;
        }
        return CompletableFuture.completedFuture(null);
    }

    private void doStart(String storeId) {
        if (!started.get()) {
            return;
        }
        Instant now = clock.instant();
        Duration chosenDelay = null;
        boolean anyDueNow = false;
        for (RangeGcState state : rangeStates.values()) {
            if (state.nextDue.isAfter(now)) {
                Duration d = Duration.between(now, state.nextDue);
                if (chosenDelay == null || d.compareTo(chosenDelay) < 0) {
                    chosenDelay = d;
                }
            } else {
                anyDueNow = true;
            }
        }
        Duration delay = anyDueNow ? Duration.ZERO : (chosenDelay != null ? chosenDelay : minCleanInterval);
        log.debug("DistWorkerCleaner next round in {} ms", delay.toMillis());
        cleanerFuture = jobScheduler.schedule(() -> {
            doGC(storeId).thenRun(() -> doStart(storeId));
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Void> doGC(String storeId) {
        Collection<KVRangeSetting> leaders = findByBoundary(FULL_BOUNDARY, distWorkerClient.latestEffectiveRouter())
            .stream().filter(r -> r.leader().equals(storeId)).toList();

        Map<KVRangeId, KVRangeSetting> currentLeaderMap = new HashMap<>();
        for (KVRangeSetting rs : leaders) {
            currentLeaderMap.put(rs.id(), rs);
            RangeGcState state = rangeStates.get(rs.id());
            if (state == null) {
                state = new RangeGcState();
                state.step = MAX_STEP;
                state.interval = minCleanInterval;
                state.lowRatioHits = 0;
                state.nextDue = Instant.EPOCH; // run asap
                state.ver = rs.ver();
                state.boundary = rs.boundary();
                state.wrappedOnce = false;
                rangeStates.put(rs.id(), state);
            } else {
                if (state.ver != rs.ver() || !state.boundary.equals(rs.boundary())) {
                    // clamp nextKey into new boundary and reset step/interval
                    if (state.nextKey != null) {
                        state.nextKey = clampToBoundary(state.nextKey, rs.boundary());
                    }
                    state.step = 1;
                    state.interval = minCleanInterval;
                    state.lowRatioHits = 0;
                    state.wrappedOnce = false;
                    state.ver = rs.ver();
                    state.boundary = rs.boundary();
                }
            }
        }
        // remove states for ranges not leader anymore
        rangeStates.keySet().removeIf(rid -> !currentLeaderMap.containsKey(rid));

        long reqId = HLC.INST.getPhysical();
        Instant now = clock.instant();
        List<CompletableFuture<?>> futures = leaders.stream()
            .map(rs -> {
                RangeGcState s = rangeStates.get(rs.id());
                if (s == null) {
                    return CompletableFuture.<Void>completedFuture(null);
                }
                if (s.nextDue.isAfter(now)) {
                    return CompletableFuture.<Void>completedFuture(null);
                }
                GCRequest.Builder gcReq = GCRequest.newBuilder().setReqId(reqId)
                    .setStepHint(s.step)
                    .setScanQuota(SCAN_QUOTA_BASE * s.step);
                if (s.nextKey != null) {
                    ByteString start = clampToBoundary(s.nextKey, rs.boundary());
                    if (start != null) {
                        gcReq.setStartKey(start);
                    }
                }
                String rangeIdStr = KVRangeIdUtil.toString(rs.id());
                log.debug("[DistWorker] start gc: reqId={}, rangeId={}, step={}, quota={}",
                    reqId, rangeIdStr, s.step, SCAN_QUOTA_BASE * s.step);
                return distWorkerClient.query(rs.leader(), KVRangeRORequest.newBuilder()
                        .setReqId(reqId)
                        .setKvRangeId(rs.id())
                        .setVer(rs.ver())
                        .setRoCoProc(ROCoProcInput.newBuilder()
                            .setDistService(DistServiceROCoProcInput.newBuilder()
                                .setGc(gcReq.build())
                                .build())
                            .build())
                        .build())
                    .handle((v, e) -> {
                        try {
                            if (e != null) {
                                log.debug("[DistWorker] gc error: reqId={}, rangeId={}", reqId, rangeIdStr, e);
                                onGCFailure(s);
                                return null;
                            }
                            if (v.getCode() != ReplyCode.Ok) {
                                log.debug("[DistWorker] gc rejected: reqId={}, rangeId={}, reason={}", reqId,
                                    rangeIdStr, v.getCode());
                                onGCFailure(s);
                                return null;
                            }
                            GCReply reply = v.getRoCoProcResult().getDistService().getGc();
                            onGCSuccess(s, reply);
                            log.debug(
                                "[DistWorker] gc done: reqId={}, rangeId={}, inspected={}, removeSuccess={}, wrapped={}",
                                reqId, rangeIdStr, reply.getInspectedCount(), reply.getRemoveSuccess(),
                                reply.getWrapped());
                        } finally {
                            s.nextDue = clock.instant().plus(s.interval);
                        }
                        return null;
                    });
            })
            .toList();
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private void onGCFailure(RangeGcState s) {
        // reset on failure
        s.step = 1;
        s.interval = minCleanInterval;
        s.lowRatioHits = 0;
        s.wrappedOnce = false;
    }

    private void onGCSuccess(RangeGcState s, GCReply reply) {
        if (reply.hasNextStartKey()) {
            s.nextKey = clampToBoundary(reply.getNextStartKey(), s.boundary);
        } else {
            s.nextKey = null;
        }
        if (reply.getWrapped()) {
            s.wrappedOnce = true;
        }
        int inspected = reply.getInspectedCount();
        int removeSuccess = reply.getRemoveSuccess();
        double successRatio = inspected == 0 ? 0 : ((double) removeSuccess) / inspected;
        int scanQuota = SCAN_QUOTA_BASE * Math.max(1, s.step);

        boolean coverageOK = s.wrappedOnce || inspected >= (int) (scanQuota * QUOTA_HIT_RATIO);
        // Shrink when removal success ratio is high
        if (successRatio >= HIGH_SUCCESS_RATIO) {
            s.step = Math.max(s.step / 2, 1);
            long shrunk = (long) (s.interval.toMillis() / SHRINK);
            s.interval = Duration.ofMillis(Math.max(shrunk, minCleanInterval.toMillis()));
            s.lowRatioHits = 0;
            s.wrappedOnce = false;
        } else {
            // Grow only when no removals for sustained rounds with sufficient coverage and a full wrap
            if (coverageOK && removeSuccess == 0) {
                s.lowRatioHits++;
                if (s.lowRatioHits >= LOW_HIT_THRESHOLD && s.wrappedOnce) {
                    // grow step only when quota is sufficiently utilized
                    if (inspected >= (int) (scanQuota * QUOTA_HIT_RATIO)) {
                        s.step = Math.min(s.step + 1, MAX_STEP);
                    }
                    long grown = (long) (s.interval.toMillis() * GROWTH);
                    s.interval = Duration.ofMillis(Math.min(grown, maxCleanInterval.toMillis()));
                    s.lowRatioHits = 0;
                    s.wrappedOnce = false; // require another full wrap before next growth
                }
            } else {
                // If not yet wrapped once and this round didn't wrap, accelerate to finish a full circle
                if (!s.wrappedOnce) {
                    s.interval = minCleanInterval;
                }
            }
        }
    }

    private ByteString clampToBoundary(ByteString key, Boundary boundary) {
        if (key == null) {
            return null;
        }
        ByteString startBoundary = BoundaryUtil.startKey(boundary);
        ByteString endBoundary = BoundaryUtil.endKey(boundary);
        if (startBoundary != null && BoundaryUtil.compare(key, startBoundary) < 0) {
            return startBoundary;
        }
        if (endBoundary != null && BoundaryUtil.compare(key, endBoundary) >= 0) {
            return startBoundary;
        }
        return key;
    }

    private static class RangeGcState {
        ByteString nextKey;
        int step;
        Duration interval;
        int lowRatioHits;
        boolean wrappedOnce;
        Instant nextDue;
        long ver;
        Boundary boundary;
    }
}
