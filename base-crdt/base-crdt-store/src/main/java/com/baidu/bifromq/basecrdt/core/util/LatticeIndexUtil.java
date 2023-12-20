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

package com.baidu.bifromq.basecrdt.core.util;

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LatticeIndexUtil {
    public static void remember(Map<ByteString, NavigableMap<Long, Long>> historyMap,
                                ByteString replicaId, long ver) {
        // handle history before n
        historyMap.compute(replicaId, (k, history) -> {
            if (history == null) {
                history = new ConcurrentSkipListMap<>();
            }
            if (history.containsKey(ver)) {
                return history;
            }
            remember(history, ver);
            return history;
        });
    }

    public static void remember(Map<ByteString, NavigableMap<Long, Long>> historyMap,
                                ByteString replicaId, long startVer, long endVer) {
        // handle history before n
        historyMap.compute(replicaId, (k, history) -> {
            if (history == null) {
                history = new ConcurrentSkipListMap<>();
            }
            remember(history, startVer, endVer);
            return history;
        });

    }

    static void remember(NavigableMap<Long, Long> ranges, long ver) {
        remember(ranges, ver, ver);
    }

    static void remember(NavigableMap<Long, Long> ranges, long startVer, long endVer) {
        Long startKey = ranges.floorKey(startVer);
        if (startKey == null) {
            startKey = startVer;
        } else {
            if (ranges.get(startKey) + 1 < startVer) {
                startKey = startVer;
            }
        }
        Long endKey = ranges.floorKey(endVer);
        if (endKey == null) {
            if (ranges.containsKey(endVer + 1)) {
                endKey = ranges.get(endVer + 1);
            } else {
                endKey = endVer;
            }
        } else {
            endKey = Math.max(endVer, ranges.get(endKey));
            if (ranges.containsKey(endKey + 1)) {
                endKey = ranges.get(endKey + 1);
            }
        }
        ranges.subMap(startKey, true, endKey, true).clear();
        ranges.put(startKey, endKey);
    }
}
