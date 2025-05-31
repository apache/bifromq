/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basekv.utils;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.endKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.startKey;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Utilities for processing descriptor.
 */
public class DescriptorUtil {
    /**
     * Organize storeDescriptors by epoch.
     *
     * @param storeDescriptors storeDescriptors
     * @return storeDescriptors organized by epoch
     */
    public static NavigableMap<Long, Set<KVRangeStoreDescriptor>> organizeByEpoch(
        Set<KVRangeStoreDescriptor> storeDescriptors) {
        NavigableMap<Long, Set<KVRangeStoreDescriptor>> epochMap = new TreeMap<>();
        // epoch -> storeId -> storeDescBuilder
        Map<Long, Map<String, KVRangeStoreDescriptor.Builder>> storeDescBuilderByEpoch = new HashMap<>();

        for (KVRangeStoreDescriptor storeDescriptor : storeDescriptors) {
            for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                long epoch = rangeDescriptor.getId().getEpoch();
                storeDescBuilderByEpoch.computeIfAbsent(epoch, e -> storeDescriptors.stream()
                        .collect(Collectors.toMap(KVRangeStoreDescriptor::getId, k -> k.toBuilder().clearRanges())))
                    .get(storeDescriptor.getId())
                    .addRanges(rangeDescriptor);
            }
        }
        storeDescBuilderByEpoch.forEach((epoch, storeDescBuilderMap) -> {
            Set<KVRangeStoreDescriptor> storeDescSet = storeDescBuilderMap.values().stream()
                .map(KVRangeStoreDescriptor.Builder::build).collect(Collectors.toSet());
            epochMap.put(epoch, storeDescSet);
        });
        return epochMap;
    }

    /**
     * Get the storeDescriptors with the least epoch.
     *
     * @param storeDescriptors storeDescriptors
     * @return storeDescriptors with the least epoch
     */
    public static Optional<EffectiveEpoch> getEffectiveEpoch(Set<KVRangeStoreDescriptor> storeDescriptors) {
        NavigableMap<Long, Set<KVRangeStoreDescriptor>> storeDescriptorsByEpoch = organizeByEpoch(storeDescriptors);
        if (storeDescriptorsByEpoch.isEmpty()) {
            return Optional.empty();
        }
        Map.Entry<Long, Set<KVRangeStoreDescriptor>> oldestEpoch = storeDescriptorsByEpoch.firstEntry();
        return Optional.of(new EffectiveEpoch(oldestEpoch.getKey(), oldestEpoch.getValue()));
    }

    /**
     * Get the effective route from the effective epoch.
     *
     * @param effectiveEpoch effective epoch
     * @return effective route
     */
    public static EffectiveRoute getEffectiveRoute(EffectiveEpoch effectiveEpoch) {
        NavigableSet<LeaderRange> firstLeaderRanges = new TreeSet<>(
            Comparator.comparingLong(l -> l.descriptor().getId().getId()));
        NavigableMap<ByteString, NavigableSet<LeaderRange>> sortedLeaderRanges = new TreeMap<>(
            ByteString.unsignedLexicographicalComparator());
        for (KVRangeStoreDescriptor storeDescriptor : effectiveEpoch.storeDescriptors()) {
            for (KVRangeDescriptor rangeDescriptor : storeDescriptor.getRangesList()) {
                if (rangeDescriptor.getRole() == RaftNodeStatus.Leader) {
                    ByteString startKey = startKey(rangeDescriptor.getBoundary());
                    if (startKey == null) {
                        firstLeaderRanges.add(new LeaderRange(rangeDescriptor, storeDescriptor));
                        continue;
                    }
                    sortedLeaderRanges.computeIfAbsent(startKey,
                            k -> new TreeSet<>(Comparator.comparingLong(l -> l.descriptor().getId().getId())))
                        .add(new LeaderRange(rangeDescriptor, storeDescriptor));
                }
            }
        }
        NavigableMap<Boundary, LeaderRange> effectiveRouteMap = new TreeMap<>(BoundaryUtil::compare);
        LeaderRange prev = firstLeaderRanges.pollFirst();
        if (prev == null) {
            Map.Entry<ByteString, NavigableSet<LeaderRange>> firstEntry = sortedLeaderRanges.firstEntry();
            if (firstEntry != null) {
                prev = firstEntry.getValue().pollFirst();
            }
        }
        while (prev != null) {
            effectiveRouteMap.put(prev.descriptor().getBoundary(), prev);
            ByteString endKey = endKey(prev.descriptor().getBoundary());
            if (endKey == null) {
                // reach the end bound
                break;
            }
            Map.Entry<ByteString, NavigableSet<LeaderRange>> next = sortedLeaderRanges.ceilingEntry(endKey);
            if (next != null) {
                prev = next.getValue().pollFirst();
            } else {
                prev = null;
            }
        }
        return new EffectiveRoute(effectiveEpoch.epoch(), effectiveRouteMap);
    }
}
