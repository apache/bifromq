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

package org.apache.bifromq.basecrdt.core.internal;

import static java.util.Collections.emptyNavigableMap;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;

class EventHistoryUtil {
    static NavigableMap<Long, Long> common(NavigableMap<Long, Long> historyA, NavigableMap<Long, Long> historyB) {
        NavigableMap<Long, Long> commonHistory = Maps.newTreeMap();
        for (Map.Entry<Long, Long> br : historyB.entrySet()) {
            long bStartKey = br.getKey();
            long bEndKey = br.getValue();
            Long aStartKey = null;
            Long aEndKey;
            if (historyA.floorKey(bStartKey) != null) {
                aStartKey = historyA.floorKey(bStartKey);
            } else if (historyA.ceilingKey(bStartKey) != null) {
                aStartKey = historyA.ceilingKey(bStartKey);
            }
            while (aStartKey != null) {
                aEndKey = historyA.get(aStartKey);
                if (aStartKey <= bStartKey) {
                    if (bStartKey <= aEndKey) {
                        if (bEndKey <= aEndKey) {
                            // aStartKey <= bStartKey <= bEndKey <= aEndKey
                            commonHistory.put(bStartKey, bEndKey);
                            aStartKey = null;
                        } else {
                            // aStartKey <= bStartKey <= aEndKey < bEndKey
                            commonHistory.put(bStartKey, aEndKey);
                            aStartKey = historyA.higherKey(aStartKey);
                        }
                    } else {
                        // aStartKey <= aEndKey < bStartKey <= bEndKey
                        aStartKey = null;
                    }
                } else {
                    if (bEndKey >= aStartKey) {
                        if (aEndKey <= bEndKey) {
                            // bStartKey < aStartKey <= aEndKey <= bEndKey
                            commonHistory.put(aStartKey, aEndKey);
                            aStartKey = historyA.higherKey(aStartKey);
                        } else {
                            // bStartKey < aStartKey <= bEndKey < aEndKey
                            commonHistory.put(aStartKey, bEndKey);
                            aStartKey = null;
                        }
                    } else {
                        // bStartKey <= bEndKey < aStartKey <= aEndKey
                        aStartKey = null;
                    }
                }
            }
        }
        return commonHistory;
    }

    static NavigableMap<Long, Long> diff(NavigableMap<Long, Long> historyA, NavigableMap<Long, Long> historyB) {
        historyA = Maps.newTreeMap(historyA);
        for (Map.Entry<Long, Long> br : historyB.entrySet()) {
            // exclude br from a
            long bStartKey = br.getKey();
            long bEndKey = br.getValue();
            Long aStartKey = null;
            Long aEndKey;
            if (historyA.floorKey(bStartKey) != null) {
                aStartKey = historyA.floorKey(bStartKey);
            } else if (historyA.ceilingKey(bStartKey) != null) {
                aStartKey = historyA.ceilingKey(bStartKey);
            }
            while (aStartKey != null) {
                aEndKey = historyA.get(aStartKey);
                if (aStartKey <= bStartKey) {
                    if (bStartKey <= aEndKey) {
                        if (bEndKey <= aEndKey) {
                            // aStartKey <= bStartKey <= bEndKey <= aEndKey
                            historyA.remove(aStartKey);
                            if (aStartKey < bStartKey) {
                                historyA.put(aStartKey, bStartKey - 1);
                            }
                            if (bEndKey < aEndKey) {
                                historyA.put(bEndKey + 1, aEndKey);
                            }
                            aStartKey = null;
                        } else {
                            // aStartKey <= bStartKey <= aEndKey < bEndKey
                            historyA.remove(aStartKey);
                            if (aStartKey < bStartKey) {
                                historyA.put(aStartKey, bStartKey - 1);
                            }
                            aStartKey = historyA.ceilingKey(bStartKey);
                        }
                    } else {
                        // aStartKey <= aEndKey < bStartKey <= bEndKey
                        aStartKey = historyA.higherKey(aStartKey);
                    }
                } else {
                    if (aStartKey <= bEndKey) {
                        if (aEndKey <= bEndKey) {
                            // bStartKey < aStartKey <= aEndKey <= bEndKey
                            historyA.remove(aStartKey);
                            aStartKey = historyA.higherKey(aStartKey);
                        } else {
                            // bStartKey < aStartKey <= bEndKey < aEndKey
                            historyA.remove(aStartKey);
                            historyA.put(bEndKey + 1, aEndKey);
                            aStartKey = null;
                        }
                    } else {
                        // bStartKey <= bEndKey < aStartKey <= aEndKey
                        aStartKey = null;
                    }
                }
            }
        }
        return historyA;
    }

    static boolean isRemembering(Map<ByteString, NavigableMap<Long, Long>> eventIndex, ByteString replicaId, long ver) {
        NavigableMap<Long, Long> ranges = eventIndex.getOrDefault(replicaId, emptyNavigableMap());
        Long key = ranges.floorKey(ver);
        return key != null && ranges.get(key) >= ver;
    }

    static void forget(Map<ByteString, NavigableMap<Long, Long>> historyMap, ByteString replicaId, long ver) {
        historyMap.computeIfPresent(replicaId, (k, v) -> {
            Map.Entry<Long, Long> b = v.floorEntry(ver);
            if (b != null) {
                // ver <= maxVer
                if (ver <= b.getValue()) {
                    v.remove(b.getKey());
                    if (b.getKey() <= ver - 1) {
                        v.put(b.getKey(), ver - 1);
                    }
                    if (ver + 1 <= b.getValue()) {
                        v.put(ver + 1, b.getValue());
                    }
                }
            }
            if (v.isEmpty()) {
                // no dot from replicaId, remove the entry
                v = null;
            }
            return v;
        });
    }

}
