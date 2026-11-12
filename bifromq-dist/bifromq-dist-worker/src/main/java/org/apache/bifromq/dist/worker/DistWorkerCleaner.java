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
import static org.apache.bifromq.basekv.utils.BoundaryUtil.clampToBoundary;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.GCReply;
import org.apache.bifromq.dist.rpc.proto.GCRequest;

@Slf4j
class DistWorkerCleaner {
    private static final int MIN_SCAN_QUOTA = 512;
    private static final int MAX_SCAN_QUOTA = 2048;
    private static final double BASE_SCAN_RATE = 64;
    private static final double HIGH_SCAN_RATE = 384;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final IBaseKVStoreClient distWorkerClient;
    private final Duration minCleanInterval;
    private final Duration maxCleanInterval;
    private final ScheduledExecutorService jobScheduler;
    private final Executor stateExecutor;
    // Insertion order is the round-robin order. Only stateExecutor accesses this map.
    private final Map<KVRangeId, RangeGcState> rangeStates = new LinkedHashMap<>();
    private ScheduledFuture<?> cleanerFuture;
    private double scanRate = BASE_SCAN_RATE;
    // Gauges are read outside stateExecutor.
    private volatile long nextDelayNanos;

    DistWorkerCleaner(IBaseKVStoreClient distWorkerClient,
                      Duration minCleanInterval,
                      Duration maxCleanInterval,
                      ScheduledExecutorService jobScheduler) {
        this.distWorkerClient = distWorkerClient;
        this.minCleanInterval = minCleanInterval;
        this.maxCleanInterval = maxCleanInterval;
        this.jobScheduler = jobScheduler;
        stateExecutor = MoreExecutors.newSequentialExecutor(jobScheduler);
    }

    void start(String storeId) {
        if (started.compareAndSet(false, true)) {
            stateExecutor.execute(() -> schedule(storeId, maxCleanInterval));
        }
    }

    CompletableFuture<Void> stop() {
        if (!started.compareAndSet(true, false)) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        stateExecutor.execute(() -> {
            if (cleanerFuture != null) {
                cleanerFuture.cancel(false);
            }
            for (RangeGcState state : rangeStates.values()) {
                Metrics.globalRegistry.removeByPreFilterId(state.stepGauge.getId());
                Metrics.globalRegistry.removeByPreFilterId(state.intervalGauge.getId());
            }
            rangeStates.clear();
            onDone.complete(null);
        });
        return onDone;
    }

    private void schedule(String storeId, Duration delay) {
        if (!started.get()) {
            return;
        }
        nextDelayNanos = delay.toNanos();
        cleanerFuture = jobScheduler.schedule(() -> {
            if (started.get()) {
                stateExecutor.execute(() -> doGC(storeId));
            }
        }, nextDelayNanos, TimeUnit.NANOSECONDS);
    }

    private void refreshRanges(String storeId) {
        Set<KVRangeId> leaders = new HashSet<>();
        for (KVRangeSetting setting : findByBoundary(FULL_BOUNDARY, distWorkerClient.latestEffectiveRouter())) {
            if (!setting.leader().equals(storeId)) {
                continue;
            }
            leaders.add(setting.id());
            RangeGcState state = rangeStates.get(setting.id());
            if (state == null) {
                state = new RangeGcState();
                state.ver = setting.ver();
                state.boundary = setting.boundary();
                Tags tags = Tags.of("storeId", storeId).and("rangeId", KVRangeIdUtil.toString(setting.id()));
                state.stepGauge = Gauge.builder("dist.gc.step", state, s -> 1).tags(tags).register(Metrics.globalRegistry);
                state.intervalGauge = Gauge.builder("dist.gc.interval.ms", this,
                        c -> c.nextDelayNanos / 1_000_000.0)
                    .tags(tags).register(Metrics.globalRegistry);
                rangeStates.put(setting.id(), state);
            } else if (state.ver != setting.ver() || !state.boundary.equals(setting.boundary())) {
                if (state.nextKey != null) {
                    state.nextKey = clampToBoundary(state.nextKey, setting.boundary());
                }
                state.ver = setting.ver();
                state.boundary = setting.boundary();
            }
        }
        rangeStates.entrySet().removeIf(entry -> {
            if (leaders.contains(entry.getKey())) {
                return false;
            }
            Metrics.globalRegistry.removeByPreFilterId(entry.getValue().stepGauge.getId());
            Metrics.globalRegistry.removeByPreFilterId(entry.getValue().intervalGauge.getId());
            return true;
        });
    }

    private void doGC(String storeId) {
        if (!started.get()) {
            return;
        }
        refreshRanges(storeId);
        if (rangeStates.isEmpty()) {
            scanRate = BASE_SCAN_RATE;
            schedule(storeId, maxCleanInterval);
            return;
        }
        Iterator<Map.Entry<KVRangeId, RangeGcState>> iterator = rangeStates.entrySet().iterator();
        Map.Entry<KVRangeId, RangeGcState> entry = iterator.next();
        KVRangeId rangeId = entry.getKey();
        RangeGcState state = entry.getValue();
        iterator.remove();
        rangeStates.put(rangeId, state);
        long ver = state.ver;
        Boundary boundary = state.boundary;
        long reqId = HLC.INST.getPhysical();
        GCRequest.Builder request = GCRequest.newBuilder().setReqId(reqId)
            .setStepHint(1).setScanQuota(MAX_SCAN_QUOTA);
        if (state.nextKey != null) {
            request.setStartKey(state.nextKey);
        }
        long startNanos = System.nanoTime();
        distWorkerClient.query(storeId, KVRangeRORequest.newBuilder()
                .setReqId(reqId).setKvRangeId(rangeId).setVer(ver)
                .setRoCoProc(ROCoProcInput.newBuilder()
                    .setDistService(DistServiceROCoProcInput.newBuilder().setGc(request)))
                .build())
            .whenComplete((reply, error) -> {
                if (!started.get()) {
                    return;
                }
                stateExecutor.execute(() -> {
                    if (!started.get()) {
                        return;
                    }
                    // Recheck the route before accepting a reply from an old leader or range version.
                    refreshRanges(storeId);
                    boolean current = rangeStates.get(rangeId) == state
                        && state.ver == ver && state.boundary.equals(boundary);
                    Duration delay;
                    if (error != null || reply.getCode() != ReplyCode.Ok || !current) {
                        scanRate = BASE_SCAN_RATE;
                        delay = maxCleanInterval;
                        log.debug("[DistWorker] gc did not complete for current range: reqId={}, rangeId={}",
                            reqId, KVRangeIdUtil.toString(rangeId), error);
                    } else {
                        delay = onGCSuccess(state, reply);
                    }
                    log.debug("[DistWorker] gc completed: reqId={}, rangeId={}, quota={}, elapsedNanos={}, "
                            + "rate={}, waitNanos={}",
                        reqId, KVRangeIdUtil.toString(rangeId), MAX_SCAN_QUOTA, System.nanoTime() - startNanos,
                        scanRate, delay.toNanos());
                    schedule(storeId, delay);
                });
            });
    }

    private Duration onGCSuccess(RangeGcState state, KVRangeROReply response) {
        GCReply reply = response.getRoCoProcResult().getDistService().getGc();
        if (!reply.getWrapped() && reply.hasNextStartKey()) {
            state.nextKey = clampToBoundary(reply.getNextStartKey(), state.boundary);
        } else {
            state.nextKey = null;
        }
        long inspected = Integer.toUnsignedLong(reply.getInspectedCount());
        long removed = Integer.toUnsignedLong(reply.getRemoveSuccess());
        if (inspected == 0) {
            scanRate = BASE_SCAN_RATE;
            return maxCleanInterval;
        }
        double successPerKey = (double) removed / inspected;
        if (removed <= 1 || successPerKey <= 0.005) {
            scanRate = Math.max(BASE_SCAN_RATE, scanRate / 1.5);
        } else if (removed >= 8 && successPerKey >= 0.03) {
            scanRate = Math.min(HIGH_SCAN_RATE, scanRate * 1.5);
        }
        long delayNanos = (long) Math.ceil(Math.max(inspected, MIN_SCAN_QUOTA) * 1_000_000_000.0 / scanRate);
        Duration delay = Duration.ofNanos(delayNanos);
        if (delay.compareTo(minCleanInterval) < 0) {
            delay = minCleanInterval;
        }
        if (delay.compareTo(maxCleanInterval) > 0) {
            delay = maxCleanInterval;
        }
        log.debug("[DistWorker] gc progress: inspected={}, removed={}, wrapped={}",
            inspected, removed, reply.getWrapped());
        return delay;
    }

    private static class RangeGcState {
        ByteString nextKey;
        long ver;
        Boundary boundary;
        Gauge stepGauge;
        Gauge intervalGauge;
    }
}
