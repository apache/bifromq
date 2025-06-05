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

package org.apache.bifromq.basekv.utils;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.KVRangeId;

/**
 * Utility class for generating and manipulating KVRangeId objects.
 */
public final class KVRangeIdUtil {
    /**
     * Generate the first KVRangeId in new epoch.
     *
     * @return a new KVRangeId object
     */
    public static KVRangeId generate() {
        long hlc = HLC.INST.get();
        return KVRangeId.newBuilder()
            .setEpoch(hlc)
            .setId(0)
            .build();
    }

    /**
     * Generate the next KVRangeId in the same epoch.
     *
     * @param from the KVRangeId to generate the next one from
     * @return a new KVRangeId object
     */
    public static KVRangeId next(KVRangeId from) {
        return next(from.getEpoch());
    }

    public static KVRangeId next(long epoch) {
        return KVRangeId.newBuilder()
            .setEpoch(epoch) // under same epoch
            .setId(HLC.INST.get())
            .build();
    }

    public static String toString(KVRangeId kvRangeId) {
        return Long.toUnsignedString(kvRangeId.getEpoch()) + "_" + Long.toUnsignedString(kvRangeId.getId());
    }

    public static KVRangeId fromString(String id) {
        String[] parts = id.split("_");
        assert parts.length == 2;
        return KVRangeId.newBuilder()
            .setEpoch(Long.parseUnsignedLong(parts[0]))
            .setId(Long.parseUnsignedLong(parts[1]))
            .build();
    }
}
