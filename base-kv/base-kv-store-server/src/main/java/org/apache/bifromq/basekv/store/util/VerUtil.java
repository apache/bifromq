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

package org.apache.bifromq.basekv.store.util;

public class VerUtil {

    /**
     * The version's semantic is as follows.
     * - high 32 bits: high version is used for tracking boundary change
     * - low 32 bits: low version is used for tracking config change
     *
     * @param ver the version to bump
     * @param bumpHigh true to bump high version, false to bump low version
     * @return the bumped version
     */
    public static long bump(long ver, boolean bumpHigh) {
        long high = ver >>> 32;
        long low = ver & 0xFFFFFFFFL;

        if (bumpHigh) {
            high = (high + 1) & 0xFFFFFFFFL;
            low = 0;
        } else {
            low = (low + 1) & 0xFFFFFFFFL;
        }
        return (high << 32) | low;
    }

    /**
     * Check if two versions corresponds to identical boundaries.
     *
     * @param ver1 the first version
     * @param ver2 the second version
     * @return true if the two versions corresponds to identical boundaries, false otherwise
     */
    public static boolean boundaryCompatible(long ver1, long ver2) {
        return (ver1 >>> 32) == (ver2 >>> 32);
    }

    public static String print(long ver) {
        return (ver >>> 32) + "-" + (ver & 0xFFFFFFFFL);
    }
}
