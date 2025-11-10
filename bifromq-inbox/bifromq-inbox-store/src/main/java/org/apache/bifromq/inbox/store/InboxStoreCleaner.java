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
import static org.apache.bifromq.basekv.utils.BoundaryUtil.clampToBoundary;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.bifromq.inbox.storage.proto.GCReply;
import org.apache.bifromq.inbox.storage.proto.GCRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;

@Slf4j
class InboxStoreCleaner {
    private static final int MAX_STEP = 10;
    private static final int SCAN_QUOTA_BASE = 512;
    private static final int FIRST_SWEEP_SCAN_QUOTA = 50_000;
    private static final Duration FIRST_SWEEP_INTERVAL = Duration.ofSeconds(5);
    private static final double HIGH_SUCCESS_RATIO = 0.005; // 0.5%
    private static final int LOW_HIT_THRESHOLD = 3;
    private static final double QUOTA_HIT_RATIO = 0.8;
    private static final double GROWTH = 1.5;
    private static final double SHRINK = 2.0;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final IBaseKVStoreClient inboxStoreClient;
    private final Duration minCleanInterval;
    private final Duration maxCleanInterval;
    private final ScheduledExecutorService jobScheduler;
    private final Clock clock;
    // rangeId -> state
    private final Map<KVRangeId, RangeGcState> rangeStates = new HashMap<>();
    private volatile ScheduledFuture<?> cleanerFuture;

    InboxStoreCleaner(IBaseKVStoreClient inboxStoreClient,
                      Duration minCleanInterval,
                      Duration maxCleanInterval,
                      ScheduledExecutorService jobScheduler) {
        this(inboxStoreClient, minCleanInterval, maxCleanInterval, jobScheduler, Clock.systemUTC());
    }

    InboxStoreCleaner(IBaseKVStoreClient inboxStoreClient,
                      Duration minCleanInterval,
                      Duration maxCleanInterval,
                      ScheduledExecutorService jobScheduler,
                      Clock clock) {
        this.inboxStoreClient = inboxStoreClient;
        this.minCleanInterval = minCleanInterval;
        this.maxCleanInterval = maxCleanInterval;
        this.jobScheduler = jobScheduler;
        this.clock = clock;
    }

    private static boolean equalsBoundary(Boundary a, Boundary b) {
        return BoundaryUtil.compareStartKey(BoundaryUtil.startKey(a), BoundaryUtil.startKey(b)) == 0
            && BoundaryUtil.compareEndKeys(BoundaryUtil.endKey(a), BoundaryUtil.endKey(b)) == 0;
    }

    void start(String storeId) {
        if (started.compareAndSet(false, true)) {
            log.info("InboxStoreCleaner started");
            doStart(storeId);
        }
    }

    CompletableFuture<Void> stop() {
        if (started.compareAndSet(true, false)) {
            cleanerFuture.cancel(true);
            CompletableFuture<Void> onDone = new CompletableFuture<>();
            jobScheduler.execute(() -> {
                log.info("InboxStoreCleaner stopped");
                // remove all gauges on stop
                for (RangeGcState st : rangeStates.values()) {
                    if (st.stepGauge != null) {
                        Metrics.globalRegistry.removeByPreFilterId(st.stepGauge.getId());
                    }
                    if (st.intervalGauge != null) {
                        Metrics.globalRegistry.removeByPreFilterId(st.intervalGauge.getId());
                    }
                }
                rangeStates.clear();
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
        Duration delay = anyDueNow ? Duration.ZERO : chosenDelay == null ? minCleanInterval : chosenDelay;
        cleanerFuture = jobScheduler.schedule(() -> {
            doClean(storeId).thenRun(() -> doStart(storeId));
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Void> doClean(String storeId) {
        if (!started.get()) {
            return CompletableFuture.completedFuture(null);
        }
        Collection<KVRangeSetting> leaders = findByBoundary(FULL_BOUNDARY, inboxStoreClient.latestEffectiveRouter())
            .stream().filter(r -> r.leader().equals(storeId)).toList();
        Map<KVRangeId, KVRangeSetting> currentLeaderMap = new HashMap<>();
        for (KVRangeSetting rs : leaders) {
            currentLeaderMap.put(rs.id(), rs);
            RangeGcState state = rangeStates.computeIfAbsent(rs.id(), rid -> {
                RangeGcState ns = new RangeGcState();
                ns.step = 1;
                ns.interval = FIRST_SWEEP_INTERVAL;
                ns.lowRatioHits = 0;
                ns.wrappedOnce = false;
                ns.nextDue = clock.instant();
                ns.ver = rs.ver();
                ns.boundary = rs.boundary();
                ns.sprinted = false;
                // register per-range gauges when state created
                Tags tags = Tags.of("storeId", storeId).and("rangeId", KVRangeIdUtil.toString(rs.id()));
                ns.intervalGauge = Gauge.builder("inbox.gc.interval.ms", ns, s -> s.interval.toMillis())
                    .tags(tags)
                    .register(Metrics.globalRegistry);
                ns.stepGauge = Gauge.builder("inbox.gc.step", ns, s -> (double) s.step)
                    .tags(tags)
                    .register(Metrics.globalRegistry);
                return ns;
            });
            if (state.ver != rs.ver() || !equalsBoundary(state.boundary, rs.boundary())) {
                if (state.nextKey != null) {
                    state.nextKey = clampToBoundary(state.nextKey, rs.boundary());
                }
                state.step = 1;
                state.interval = state.sprinted ? minCleanInterval : FIRST_SWEEP_INTERVAL;
                state.lowRatioHits = 0;
                state.wrappedOnce = false;
                state.ver = rs.ver();
                state.boundary = rs.boundary();
            }
        }
        // clean up metrics for ranges no longer led by this store
        Iterator<Map.Entry<KVRangeId, RangeGcState>> iter = rangeStates.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<KVRangeId, RangeGcState> e = iter.next();
            if (!currentLeaderMap.containsKey(e.getKey())) {
                RangeGcState st = e.getValue();
                if (st.stepGauge != null) {
                    Metrics.globalRegistry.removeByPreFilterId(st.stepGauge.getId());
                }
                if (st.intervalGauge != null) {
                    Metrics.globalRegistry.removeByPreFilterId(st.intervalGauge.getId());
                }
                iter.remove();
            }
        }

        long reqId = HLC.INST.getPhysical();
        Instant now = clock.instant();
        CompletableFuture<?>[] futures = leaders.stream()
            .map(rs -> {
                RangeGcState s = rangeStates.get(rs.id());
                if (s == null || s.nextDue.isAfter(now)) {
                    return CompletableFuture.completedFuture(null);
                }
                GCRequest.Builder gcReq = GCRequest.newBuilder().setNow(now.toEpochMilli());
                int quotaToUse = s.sprinted ? SCAN_QUOTA_BASE * s.step : FIRST_SWEEP_SCAN_QUOTA;
                gcReq.setScanQuota(quotaToUse);
                if (s.nextKey != null) {
                    ByteString start = clampToBoundary(s.nextKey, rs.boundary());
                    if (start != null) {
                        gcReq.setStartKey(start);
                    }
                }
                String rangeIdStr = KVRangeIdUtil.toString(rs.id());
                log.debug("[InboxStore] start gc: reqId={}, rangeId={}, step={}, quota={}",
                    reqId, rangeIdStr, s.step, quotaToUse);
                return inboxStoreClient.query(rs.leader(), KVRangeRORequest.newBuilder()
                        .setReqId(reqId)
                        .setKvRangeId(rs.id())
                        .setVer(rs.ver())
                        .setRoCoProc(ROCoProcInput.newBuilder()
                            .setInboxService(InboxServiceROCoProcInput.newBuilder()
                                .setReqId(reqId)
                                .setGc(gcReq.build())
                                .build())
                            .build())
                        .build())
                    .handle((v, e) -> {
                        try {
                            if (e != null) {
                                log.debug("[InboxStore] gc error: reqId={}, rangeId={}", reqId, rangeIdStr, e);
                                onGCFailure(s);
                                return null;
                            }
                            if (v.getCode() != ReplyCode.Ok) {
                                log.debug("[InboxStore] gc rejected: reqId={}, rangeId={}, reason={}", reqId,
                                    rangeIdStr, v.getCode());
                                onGCFailure(s);
                                return null;
                            }
                            GCReply reply = v.getRoCoProcResult().getInboxService().getGc();
                            onGCSuccess(s, reply);
                            log.debug(
                                "[InboxStore] gc done: reqId={}, rangeId={}, inspected={}, removed={}, wrapped={}",
                                reqId, rangeIdStr, reply.getInspectedCount(), reply.getRemoveSuccess(),
                                reply.getWrapped());
                        } finally {
                            s.nextDue = clock.instant().plus(s.interval);
                        }
                        return null;
                    });
            })
            .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures);
    }

    private void onGCFailure(RangeGcState s) {
        s.step = 1;
        s.interval = s.sprinted ? minCleanInterval : FIRST_SWEEP_INTERVAL;
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
        if (!s.sprinted) {
            if (reply.getWrapped()) {
                s.sprinted = true;
                s.step = 1;
                s.interval = minCleanInterval;
                s.lowRatioHits = 0;
                s.wrappedOnce = false;
            }
            return;
        }
        int inspected = reply.getInspectedCount();
        int removeSuccess = reply.getRemoveSuccess();
        double successRatio = inspected == 0 ? 0 : ((double) removeSuccess) / inspected;
        int scanQuota = SCAN_QUOTA_BASE * Math.max(1, s.step);

        boolean coverageOK = s.wrappedOnce || inspected >= (int) (scanQuota * QUOTA_HIT_RATIO);
        if (successRatio >= HIGH_SUCCESS_RATIO) {
            s.step = Math.max(s.step / 2, 1);
            long shrunk = (long) (s.interval.toMillis() / SHRINK);
            s.interval = Duration.ofMillis(Math.max(shrunk, minCleanInterval.toMillis()));
            s.lowRatioHits = 0;
            s.wrappedOnce = false;
        } else {
            if (coverageOK && removeSuccess == 0) {
                s.lowRatioHits++;
                if (s.lowRatioHits >= LOW_HIT_THRESHOLD && s.wrappedOnce) {
                    if (inspected >= (int) (scanQuota * QUOTA_HIT_RATIO)) {
                        s.step = Math.min(s.step + 1, MAX_STEP);
                    }
                    long grown = (long) (s.interval.toMillis() * GROWTH);
                    s.interval = Duration.ofMillis(Math.min(grown, maxCleanInterval.toMillis()));
                    s.lowRatioHits = 0;
                    s.wrappedOnce = false;
                }
            } else {
                if (!s.wrappedOnce) {
                    s.interval = minCleanInterval;
                }
            }
        }
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
        boolean sprinted;
        // metrics
        Gauge stepGauge; // gauge for adaptive step
        Gauge intervalGauge; // gauge for gc interval in ms
    }
}
