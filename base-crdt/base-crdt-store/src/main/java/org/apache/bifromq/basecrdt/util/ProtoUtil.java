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

package org.apache.bifromq.basecrdt.util;

import org.apache.bifromq.basecrdt.store.proto.EventIndex;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class ProtoUtil {
    public static List<EventIndex> to(Map<ByteString, NavigableMap<Long, Long>> latticeEvents) {
        List<EventIndex> histories = new ArrayList<>(latticeEvents.size());
        latticeEvents.forEach((k, v) -> {
            EventIndex.Builder builder = EventIndex.newBuilder().setReplicaId(k);
            // do not use builder.putAllRanges() to avoid NPE
            v.forEach(builder::putRanges);
            histories.add(builder.build());
        });
        return histories;
    }

    public static Map<ByteString, NavigableMap<Long, Long>> to(List<EventIndex> eventIndexList) {
        Map<ByteString, NavigableMap<Long, Long>> map = Maps.newHashMap();
        for (EventIndex idx : eventIndexList) {
            Map<Long, Long> ranges = map.computeIfAbsent(idx.getReplicaId(), k -> Maps.newTreeMap());
            ranges.putAll(idx.getRangesMap());
        }
        return map;
    }
}
