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

package org.apache.bifromq.dist.worker.hinter;

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.SplitHint;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.range.hinter.IKVLoadRecord;
import org.apache.bifromq.basekv.store.range.hinter.IKVRangeSplitHinter;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.dist.rpc.proto.BatchMatchRequest;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchRequest;
import org.apache.bifromq.type.RouteMatcher;

@Slf4j
public class FanoutSplitHinter implements IKVRangeSplitHinter {
    public static final String TYPE = "fanout_split_hinter";
    public static final String LOAD_TYPE_FANOUT_TOPIC_FILTERS = "fanout_topicfilters";
    public static final String LOAD_TYPE_FANOUT_SCALE = "fanout_scale";
    private final int splitAtScale;
    private final Supplier<IKVRangeReader> readerSupplier;
    // key: matchRecordKeyPrefix, value: splitKey
    private final Map<ByteString, FanOutSplit> fanoutSplitKeys = new ConcurrentHashMap<>();
    private final Gauge fanOutTopicFiltersGauge;
    private final Gauge fanOutScaleGauge;
    private volatile Boundary boundary;

    public FanoutSplitHinter(Supplier<IKVRangeReader> readerSupplier, int splitAtScale, String... tags) {
        this.splitAtScale = splitAtScale;
        this.readerSupplier = readerSupplier;
        try (IKVRangeReader reader = readerSupplier.get()) {
            boundary = reader.boundary();
        }
        fanOutTopicFiltersGauge = Gauge.builder("dist.fanout.topicfilters", fanoutSplitKeys::size)
            .tags(tags)
            .register(Metrics.globalRegistry);
        fanOutScaleGauge = Gauge.builder("dist.fanout.scale",
                () -> fanoutSplitKeys.values()
                    .stream()
                    .map(f -> f.estimation.dataSize / f.estimation.recordSize)
                    .reduce(Long::sum).orElse(0L))
            .tags(tags)
            .register(Metrics.globalRegistry);
    }

    @Override
    public void recordQuery(ROCoProcInput input, IKVLoadRecord ioRecord) {
    }

    @Override
    public void recordMutate(RWCoProcInput input, IKVLoadRecord ioRecord) {
        assert input.hasDistService();
        switch (input.getDistService().getTypeCase()) {
            case BATCHMATCH -> {
                BatchMatchRequest request = input.getDistService().getBatchMatch();
                Map<ByteString, RecordEstimation> routeKeyLoads = new HashMap<>();
                request.getRequestsMap().forEach((tenantId, records) ->
                    records.getRouteList().forEach(route -> {
                        RouteMatcher routeMatcher = route.getMatcher();
                        if (routeMatcher.getType() == RouteMatcher.Type.Normal) {
                            ByteString routeKey = toNormalRouteKey(tenantId, routeMatcher, toReceiverUrl(route));
                            routeKeyLoads.computeIfAbsent(routeKey,
                                k -> new RecordEstimation(false)).addRecordSize(routeKey.size());
                        } else {
                            ByteString routeKey = toGroupRouteKey(tenantId, route.getMatcher());
                            routeKeyLoads.computeIfAbsent(routeKey, k -> new RecordEstimation(false))
                                .addRecordSize(routeKey.size());
                        }
                    }));
                doEstimate(routeKeyLoads);
            }
            case BATCHUNMATCH -> {
                BatchUnmatchRequest request = input.getDistService().getBatchUnmatch();
                Map<ByteString, RecordEstimation> routeKeyLoads = new HashMap<>();
                request.getRequestsMap().forEach((tenantId, records) ->
                    records.getRouteList().forEach(route -> {
                        RouteMatcher routeMatcher = route.getMatcher();
                        if (routeMatcher.getType() == RouteMatcher.Type.Normal) {
                            ByteString routeKey = toNormalRouteKey(tenantId, routeMatcher, toReceiverUrl(route));
                            routeKeyLoads.computeIfAbsent(routeKey,
                                k -> new RecordEstimation(true)).addRecordSize(routeKey.size());
                        } else {
                            ByteString routeKey = toGroupRouteKey(tenantId, route.getMatcher());
                            routeKeyLoads.computeIfAbsent(routeKey,
                                k -> new RecordEstimation(true)).addRecordSize(routeKey.size());
                        }
                    }));
                doEstimate(routeKeyLoads);
            }
            default -> {
                // ignore
            }
        }
    }

    @Override
    public void reset(Boundary boundary) {
        this.boundary = boundary;
        Map<ByteString, RecordEstimation> finished = new HashMap<>();
        for (Map.Entry<ByteString, FanOutSplit> entry : fanoutSplitKeys.entrySet()) {
            ByteString matchRecordKeyPrefix = entry.getKey();
            FanOutSplit fanoutSplit = entry.getValue();
            if (!BoundaryUtil.inRange(fanoutSplit.splitKey, boundary)) {
                fanoutSplitKeys.remove(matchRecordKeyPrefix);
                RecordEstimation recordEst = new RecordEstimation(false);
                recordEst.addRecordSize(fanoutSplit.estimation.recordSize);
                finished.put(matchRecordKeyPrefix, recordEst);
            }
        }
        // check if the finished still needs more split
        doEstimate(finished);
    }

    @Override
    public SplitHint estimate() {
        Optional<Map.Entry<ByteString, FanOutSplit>> firstSplit = fanoutSplitKeys.entrySet().stream().findFirst();
        SplitHint.Builder hintBuilder = SplitHint.newBuilder().setType(TYPE)
            .putLoad(LOAD_TYPE_FANOUT_TOPIC_FILTERS, fanoutSplitKeys.size())
            .putLoad(LOAD_TYPE_FANOUT_SCALE, 0);
        firstSplit.ifPresent(s -> {
            if (!BoundaryUtil.isSplittable(boundary, s.getValue().splitKey)) {
                fanoutSplitKeys.remove(s.getKey());
            } else {
                hintBuilder.setSplitKey(s.getValue().splitKey);
                hintBuilder.putLoad(LOAD_TYPE_FANOUT_SCALE,
                    s.getValue().estimation.dataSize / (double) s.getValue().estimation.recordSize);
            }
        });
        return hintBuilder.build();
    }

    @Override
    public void close() {
        Metrics.globalRegistry.remove(fanOutTopicFiltersGauge);
        Metrics.globalRegistry.remove(fanOutScaleGauge);
    }

    private void doEstimate(Map<ByteString, RecordEstimation> routeKeyLoads) {
        Map<ByteString, RangeEstimation> splitCandidate = new HashMap<>();
        try (IKVRangeReader reader = readerSupplier.get()) {
            routeKeyLoads.forEach((matchRecordKeyPrefix, recordEst) -> {
                long dataSize = (reader.size(Boundary.newBuilder()
                    .setStartKey(matchRecordKeyPrefix)
                    .setEndKey(BoundaryUtil.upperBound(matchRecordKeyPrefix))
                    .build())) - recordEst.tombstoneSize();
                long fanOutScale = dataSize / recordEst.avgRecordSize();
                if (fanOutScale >= splitAtScale) {
                    splitCandidate.put(matchRecordKeyPrefix, new RangeEstimation(dataSize, recordEst.avgRecordSize()));
                } else if (fanoutSplitKeys.containsKey(matchRecordKeyPrefix) && fanOutScale < 0.5 * splitAtScale) {
                    fanoutSplitKeys.remove(matchRecordKeyPrefix);
                }
            });
            if (!splitCandidate.isEmpty()) {
                try (IKVIterator itr = reader.iterator()) {
                    for (ByteString routeKey : splitCandidate.keySet()) {
                        RangeEstimation recEst = splitCandidate.get(routeKey);
                        fanoutSplitKeys.computeIfAbsent(routeKey, k -> {
                            int i = 0;
                            for (itr.seek(routeKey); itr.isValid(); itr.next()) {
                                if (i++ >= splitAtScale) {
                                    return new FanOutSplit(recEst, itr.key());
                                }
                            }
                            return null;
                        });
                    }
                }
            }
        }
    }

    private record RangeEstimation(long dataSize, int recordSize) {
    }

    private record FanOutSplit(RangeEstimation estimation, ByteString splitKey) {
    }
}
