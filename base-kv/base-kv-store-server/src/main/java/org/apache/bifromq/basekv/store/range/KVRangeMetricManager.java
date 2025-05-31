/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basekv.store.range;

import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class KVRangeMetricManager implements IKVRangeMetricManager {
    private final DistributionSummary dumpBytesSummary;
    private final DistributionSummary restoreBytesSummary;
    private final Gauge stateGauge;
    private final Gauge verGauge;
    private final Gauge lastAppliedIndexGauge;
    private final Gauge dataSizeGauge;
    private final Gauge walSizeGauge;
    private final Timer configChangeTimer;
    private final Timer transferLeaderTimer;
    private final Timer splitTimer;
    private final Timer mergeTimer;
    private final Timer putTimer;
    private final Timer deleteTimer;
    private final Timer mutateCoProcTimer;
    private final Timer existTimer;
    private final Timer getTimer;
    private final Timer queryCoProcTimer;
    private final Timer compactionTimer;
    private final Timer applyLogTimer;
    private final Timer installSnapshotTimer;
    private final AtomicReference<KVRangeDescriptor> currentDesc = new AtomicReference<>();
    private final AtomicLong currentLastAppliedIndex = new AtomicLong(-1);

    KVRangeMetricManager(String clusterId, String storeId, KVRangeId rangeId) {
        Tags tags = Tags.of("clusterId", clusterId)
            .and("storeId", storeId)
            .and("rangeId", KVRangeIdUtil.toString(rangeId));
        dumpBytesSummary = Metrics.summary("basekv.snap.dump", tags);
        restoreBytesSummary = Metrics.summary("basekv.snap.restore", tags);
        stateGauge = Gauge.builder("basekv.meta.state", () -> {
                KVRangeDescriptor desc = currentDesc.get();
                if (desc != null) {
                    return desc.getState().ordinal();
                }
                return State.StateType.NoUse.ordinal();
            })
            .tags(tags)
            .register(Metrics.globalRegistry);
        verGauge = Gauge.builder("basekv.meta.ver", () -> {
                KVRangeDescriptor desc = currentDesc.get();
                if (desc != null) {
                    return desc.getVer();
                }
                return -1;
            })
            .tags(tags)
            .register(Metrics.globalRegistry);
        lastAppliedIndexGauge = Gauge.builder("basekv.meta.appidx", currentLastAppliedIndex::get)
            .tags(tags)
            .register(Metrics.globalRegistry);
        dataSizeGauge = Gauge.builder("basekv.meta.size", () -> {
                KVRangeDescriptor desc = currentDesc.get();
                if (desc != null) {
                    return desc.getStatisticsMap().getOrDefault("dataSize", 0.0).longValue();
                }
                return 0;
            })
            .tags(tags)
            .register(Metrics.globalRegistry);
        walSizeGauge = Gauge.builder("basekv.meta.walsize", () -> {
                KVRangeDescriptor desc = currentDesc.get();
                if (desc != null) {
                    return desc.getStatisticsMap().getOrDefault("walSize", 0.0).longValue();
                }
                return 0;
            })
            .tags(tags)
            .register(Metrics.globalRegistry);
        configChangeTimer = Timer.builder("basekv.cmd.configchange")
            .tags(tags)
            .register(Metrics.globalRegistry);
        transferLeaderTimer = Timer.builder("basekv.cmd.transferleader")
            .tags(tags)
            .register(Metrics.globalRegistry);
        splitTimer = Timer.builder("basekv.cmd.split")
            .tags(tags)
            .register(Metrics.globalRegistry);
        mergeTimer = Timer.builder("basekv.cmd.merge")
            .tags(tags)
            .register(Metrics.globalRegistry);
        putTimer = Timer.builder("basekv.cmd.put")
            .tags(tags)
            .register(Metrics.globalRegistry);
        deleteTimer = Timer.builder("basekv.cmd.delete")
            .tags(tags)
            .register(Metrics.globalRegistry);
        mutateCoProcTimer = Timer.builder("basekv.cmd.mutatecoproc")
            .tags(tags)
            .register(Metrics.globalRegistry);
        existTimer = Timer.builder("basekv.cmd.exist")
            .tags(tags)
            .register(Metrics.globalRegistry);
        getTimer = Timer.builder("basekv.cmd.get")
            .tags(tags)
            .register(Metrics.globalRegistry);
        queryCoProcTimer = Timer.builder("basekv.cmd.querycoproc")
            .tags(tags)
            .register(Metrics.globalRegistry);
        compactionTimer = Timer.builder("basekv.cmd.compact")
            .tags(tags)
            .register(Metrics.globalRegistry);
        applyLogTimer = Timer.builder("basekv.applylog")
            .tags(tags)
            .register(Metrics.globalRegistry);
        installSnapshotTimer = Timer.builder("basekv.installsnapshot")
            .tags(tags)
            .register(Metrics.globalRegistry);
    }

    @Override
    public void report(KVRangeDescriptor descriptor) {
        currentDesc.set(descriptor);
    }

    @Override
    public void reportDump(int bytes) {
        dumpBytesSummary.record(bytes);
    }

    @Override
    public void reportRestore(int bytes) {
        restoreBytesSummary.record(bytes);
    }

    @Override
    public void reportLastAppliedIndex(long index) {
        currentLastAppliedIndex.set(index);
    }

    @Override
    public <T> CompletableFuture<T> recordDuration(Supplier<CompletableFuture<T>> supplier, Timer timer) {
        Timer.Sample sample = Timer.start();
        CompletableFuture<T> f = supplier.get();
        f.whenComplete((v, e) -> sample.stop(timer));
        return f;
    }

    @Override
    public CompletableFuture<Void> recordConfigChange(Supplier<CompletableFuture<Void>> supplier) {
        return recordDuration(supplier, configChangeTimer);
    }

    @Override
    public CompletableFuture<Void> recordTransferLeader(Supplier<CompletableFuture<Void>> supplier) {
        return recordDuration(supplier, transferLeaderTimer);
    }

    @Override
    public CompletableFuture<Void> recordSplit(Supplier<CompletableFuture<Void>> supplier) {
        return recordDuration(supplier, splitTimer);
    }

    @Override
    public CompletableFuture<Void> recordMerge(Supplier<CompletableFuture<Void>> supplier) {
        return recordDuration(supplier, mergeTimer);
    }

    @Override
    public CompletableFuture<ByteString> recordPut(Supplier<CompletableFuture<ByteString>> supplier) {
        return recordDuration(supplier, putTimer);
    }

    @Override
    public CompletableFuture<ByteString> recordDelete(Supplier<CompletableFuture<ByteString>> supplier) {
        return recordDuration(supplier, deleteTimer);
    }

    @Override
    public CompletableFuture<RWCoProcOutput> recordMutateCoProc(Supplier<CompletableFuture<RWCoProcOutput>> supplier) {
        return recordDuration(supplier, mutateCoProcTimer);
    }

    @Override
    public CompletableFuture<Boolean> recordExist(Supplier<CompletableFuture<Boolean>> supplier) {
        return recordDuration(supplier, existTimer);
    }

    @Override
    public CompletableFuture<Optional<ByteString>> recordGet(
        Supplier<CompletableFuture<Optional<ByteString>>> supplier) {
        return recordDuration(supplier, getTimer);
    }

    @Override
    public CompletableFuture<ROCoProcOutput> recordQueryCoProc(Supplier<CompletableFuture<ROCoProcOutput>> supplier) {
        return recordDuration(supplier, queryCoProcTimer);
    }

    @Override
    public CompletableFuture<Void> recordCompact(Supplier<CompletableFuture<Void>> supplier) {
        return recordDuration(supplier, compactionTimer);
    }

    @Override
    public CompletableFuture<Void> recordLogApply(Supplier<CompletableFuture<Void>> supplier) {
        return recordDuration(supplier, applyLogTimer);
    }

    @Override
    public CompletableFuture<Void> recordSnapshotInstall(Supplier<CompletableFuture<Void>> supplier) {
        return recordDuration(supplier, installSnapshotTimer);
    }

    void close() {
        Metrics.globalRegistry.removeByPreFilterId(dumpBytesSummary.getId());
        Metrics.globalRegistry.removeByPreFilterId(restoreBytesSummary.getId());
        Metrics.globalRegistry.removeByPreFilterId(stateGauge.getId());
        Metrics.globalRegistry.removeByPreFilterId(lastAppliedIndexGauge.getId());
        Metrics.globalRegistry.removeByPreFilterId(verGauge.getId());
        Metrics.globalRegistry.removeByPreFilterId(dataSizeGauge.getId());
        Metrics.globalRegistry.removeByPreFilterId(walSizeGauge.getId());
        Metrics.globalRegistry.removeByPreFilterId(configChangeTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(transferLeaderTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(splitTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(mergeTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(putTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(deleteTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(mutateCoProcTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(existTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(getTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(queryCoProcTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(compactionTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(applyLogTimer.getId());
        Metrics.globalRegistry.removeByPreFilterId(installSnapshotTimer.getId());
    }
}
