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

package org.apache.bifromq.basekv.store.range.hinter;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.SplitHint;
import org.apache.bifromq.basekv.store.api.IKVLoadRecord;
import org.apache.bifromq.basekv.store.api.IKVRangeSplitHinter;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class KVLoadBasedSplitHinter implements IKVRangeSplitHinter {
    public static final String LOAD_TYPE_IO_DENSITY = "ioDensity";
    public static final String LOAD_TYPE_IO_LATENCY_NANOS = "ioLatencyNanos";
    public static final String LOAD_TYPE_AVG_LATENCY_NANOS = "avgLatencyNanos";
    private final Supplier<Long> nanoSource;
    private final long windowSizeNanos;
    private final NavigableMap<Long, LoadRecordWindow> trackedKeySlots = new ConcurrentSkipListMap<>();
    private final NavigableMap<Long, SplitHint> recentLoadHints = new ConcurrentSkipListMap<>();
    private final Function<ByteString, Optional<ByteString>> toSplitKey;
    private final Gauge ioDensityGuage;
    private final Gauge ioLatencyNanosGauge;
    private final Gauge avgLatencyNanosGauge;
    private volatile SplitHint latestHint = SplitHint.getDefaultInstance();

    public KVLoadBasedSplitHinter(Supplier<Long> nanoSource,
                                  Duration windowSize,
                                  Function<ByteString, Optional<ByteString>> toSplitKey,
                                  String... tags) {
        Preconditions.checkArgument(!windowSize.isNegative(), "Window size must be positive");
        this.nanoSource = nanoSource;
        this.windowSizeNanos = windowSize.toNanos();
        this.toSplitKey = toSplitKey;
        ioDensityGuage = Gauge.builder("basekv.load.est.iodensity",
                () -> latestHint.getLoadOrDefault(LOAD_TYPE_IO_DENSITY, 0))
            .tags(tags)
            .tags("type", type())
            .register(Metrics.globalRegistry);
        ioLatencyNanosGauge = Gauge.builder("basekv.load.est.iolatency",
                () -> latestHint.getLoadOrDefault(LOAD_TYPE_IO_LATENCY_NANOS, 0))
            .tags(tags)
            .tags("type", type())
            .register(Metrics.globalRegistry);
        avgLatencyNanosGauge = Gauge.builder("basekv.load.est.avglatency", () ->
                latestHint.getLoadOrDefault(LOAD_TYPE_AVG_LATENCY_NANOS, 0))
            .tags(tags)
            .tags("type", type())
            .register(Metrics.globalRegistry);
    }

    @Override
    public void reset(Boundary boundary) {
        trackedKeySlots.clear();
        recentLoadHints.clear();
    }

    @Override
    public SplitHint estimate() {
        long currentSlot = getSlot();
        trackedKeySlots.headMap(currentSlot - 2).clear();
        recentLoadHints.headMap(currentSlot).clear();
        latestHint = recentLoadHints.computeIfAbsent(currentSlot, n -> doEstimate(n - 1));
        return latestHint;
    }

    protected void onRecord(IKVLoadRecord kvLoadRecord) {
        long startNanos = kvLoadRecord.startNanos();
        int kvIOs = kvLoadRecord.getKVIOs();
        long kvNanos = kvLoadRecord.getKVIONanos();
        Map<ByteString, Long> loadDistribution = kvLoadRecord.keyDistribution();
        long now = nanoSource.get();
        long currentSlot = now / windowSizeNanos;
        long mySlot = startNanos / windowSizeNanos;
        trackedKeySlots.headMap(currentSlot - 1).clear();
        if (mySlot < currentSlot) {
            // cross window slot
            long slotBegin = currentSlot * windowSizeNanos;
            trackedKeySlots.computeIfAbsent(currentSlot, k -> new LoadRecordWindow(toSplitKey))
                .record(loadDistribution, kvIOs, kvNanos, now - slotBegin);

            if (mySlot + 1 < currentSlot) {
                trackedKeySlots.computeIfAbsent(currentSlot - 1,
                    k -> new LoadRecordWindow(toSplitKey)).record(loadDistribution, kvIOs, kvNanos, windowSizeNanos);
            } else {
                trackedKeySlots.computeIfAbsent(currentSlot - 1,
                        k -> new LoadRecordWindow(toSplitKey))
                    .record(loadDistribution, kvIOs, kvNanos, slotBegin - startNanos);
            }
            // re-estimate
            recentLoadHints.put(currentSlot - 1, doEstimate(currentSlot - 1));
        } else {
            // still in same window slot
            trackedKeySlots.computeIfAbsent(currentSlot,
                k -> new LoadRecordWindow(toSplitKey)).record(loadDistribution, kvIOs, kvNanos, now - startNanos);
        }
    }

    protected abstract String type();

    private SplitHint doEstimate(long slot) {
        LoadRecordWindow loadHint = trackedKeySlots.get(slot);
        if (loadHint == null) {
            return SplitHint.newBuilder().setType(type())
                .putLoad(LOAD_TYPE_IO_DENSITY, 0)
                .putLoad(LOAD_TYPE_IO_LATENCY_NANOS, 0)
                .putLoad(LOAD_TYPE_AVG_LATENCY_NANOS, 0)
                .build();
        }
        SplitHint.Builder hintBuilder = SplitHint.newBuilder().setType(type());
        hintBuilder.putLoad(LOAD_TYPE_IO_DENSITY, loadHint.ioDensity());
        hintBuilder.putLoad(LOAD_TYPE_IO_LATENCY_NANOS, loadHint.ioLatencyNanos());
        hintBuilder.putLoad(LOAD_TYPE_AVG_LATENCY_NANOS, loadHint.avgLatencyNanos());
        loadHint.estimateSplitKey().ifPresent(hintBuilder::setSplitKey);
        return hintBuilder.build();
    }

    private long getSlot() {
        long nano = nanoSource.get();
        return nano / windowSizeNanos;
    }

    @Override
    public void close() {
        Metrics.globalRegistry.remove(ioDensityGuage.getId());
        Metrics.globalRegistry.remove(ioLatencyNanosGauge.getId());
        Metrics.globalRegistry.remove(avgLatencyNanosGauge.getId());
    }
}
