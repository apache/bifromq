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

package org.apache.bifromq.basekv.client;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.compareEndKeys;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.endKey;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

public class KVRangeRouterUtil {

    public static Optional<KVRangeSetting> findByKey(ByteString key,
                                                     NavigableMap<Boundary, KVRangeSetting> effectiveRouter) {
        Map.Entry<Boundary, KVRangeSetting> entry =
            effectiveRouter.floorEntry(Boundary.newBuilder().setStartKey(key).build());
        if (entry != null) {
            KVRangeSetting setting = entry.getValue();
            if (BoundaryUtil.inRange(key, entry.getKey())) {
                return Optional.of(setting);
            }
        }
        return Optional.empty();
    }

    public static Collection<KVRangeSetting> findByBoundary(Boundary boundary,
                                                            NavigableMap<Boundary, KVRangeSetting> effectiveRouter) {

        if (effectiveRouter.isEmpty()) {
            return Collections.emptyList();
        }
        // boundary: FULLBoundary -> KVRangeSetting
        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
            return effectiveRouter.values();
        }

        // boundary: (null, endKey]
        if (!boundary.hasStartKey()) {
            Boundary boundaryEnd = Boundary.newBuilder()
                .setStartKey(boundary.getEndKey())
                .setEndKey(boundary.getEndKey())
                .build();
            return effectiveRouter.headMap(boundaryEnd, false).values();
        }

        // boundary: [startKey, null)
        if (!boundary.hasEndKey()) {
            Boundary boundaryStart = Boundary.newBuilder()
                .setStartKey(boundary.getStartKey())
                .setEndKey(boundary.getStartKey())
                .build();
            Boundary floorBoundary = effectiveRouter.floorKey(boundaryStart);
            if (floorBoundary == null) {
                floorBoundary = effectiveRouter.firstKey();
            }
            boolean includeFromKey = compareEndKeys(endKey(floorBoundary), boundary.getStartKey()) > 0;
            return effectiveRouter.tailMap(floorBoundary, includeFromKey).values();
        }

        // boundary: [startKey, endKey)
        Boundary boundaryStart = Boundary.newBuilder()
            .setStartKey(boundary.getStartKey())
            .setEndKey(boundary.getStartKey())
            .build();
        Boundary boundaryEnd = Boundary.newBuilder()
            .setStartKey(boundary.getEndKey())
            .setEndKey(boundary.getEndKey())
            .build();
        Boundary floorBoundary = effectiveRouter.floorKey(boundaryStart);
        if (floorBoundary == null) {
            floorBoundary = effectiveRouter.firstKey();
        }
        boolean includeFromKey = compareEndKeys(endKey(floorBoundary), boundary.getStartKey()) > 0;
        return effectiveRouter.subMap(floorBoundary, includeFromKey, boundaryEnd, false).values();
    }
}
