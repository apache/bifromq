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

package org.apache.bifromq.basekv.localengine.memory;

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import org.apache.bifromq.basekv.proto.Boundary;

public class InMemKVHelper {
    public static long sizeOfRange(NavigableMap<ByteString, ByteString> rangeData, Boundary boundary) {
        SortedMap<ByteString, ByteString> sizedData;
        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
            sizedData = rangeData;
        } else if (!boundary.hasStartKey()) {
            sizedData = rangeData.headMap(boundary.getEndKey());
        } else if (!boundary.hasEndKey()) {
            sizedData = rangeData.tailMap(boundary.getStartKey());
        } else {
            sizedData = rangeData.subMap(boundary.getStartKey(), boundary.getEndKey());
        }
        long sum = 0L;
        for (Map.Entry<ByteString, ByteString> e : sizedData.entrySet()) {
            sum += e.getKey().size() + e.getValue().size();
        }
        return sum;
    }
}
