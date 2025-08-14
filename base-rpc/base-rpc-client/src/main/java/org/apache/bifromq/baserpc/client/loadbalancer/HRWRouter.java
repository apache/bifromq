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

package org.apache.bifromq.baserpc.client.loadbalancer;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.util.Collection;
import java.util.Objects;

/**
 * Rendezvous Hashing Weighted Router (HRW).
 */
class HRWRouter<T> {
    private static final long IEEE754_DOUBLE_1 = 0x3FF0000000000000L; // 1.0 in IEEE 754 double format
    private final Collection<T> nodes;
    private final KeyFunction<T> keyFunction;
    private final WeightFunction<T> weightFunction;
    private final HashFunction hashFunction;

    HRWRouter(Collection<T> nodes, KeyFunction<T> keyFunction, WeightFunction<T> weightFunction) {
        this(nodes, keyFunction, weightFunction, (key) -> {
            HashCode code = Hashing.murmur3_128().hashString(key, Charsets.UTF_8);
            return code.asLong();
        });
    }

    HRWRouter(Collection<T> nodes,
              KeyFunction<T> keyFunction,
              WeightFunction<T> weightFunction,
              HashFunction hashFunction) {
        this.nodes = Objects.requireNonNull(nodes);
        this.keyFunction = Objects.requireNonNull(keyFunction);
        this.weightFunction = Objects.requireNonNull(weightFunction);
        this.hashFunction = Objects.requireNonNull(hashFunction);
    }

    // map unsigned long to double in (0,1) uniformly
    private static double hashToUnitInterval(long x) {
        double u = Double.longBitsToDouble((x >>> 12) | IEEE754_DOUBLE_1) - 1.0;
        final double eps = 1e-12;
        if (u <= 0) {
            u = eps;
        }
        if (u >= 1) {
            u = 1 - eps;
        }
        return u;
    }

    /**
     * Route to the best node based on the given object key.
     *
     * @param objectKey the key to route
     * @return the best node, or null if no nodes are available
     */
    T routeNode(String objectKey) {
        if (nodes.isEmpty()) {
            return null;
        }
        T bestNode = null;
        double bestScore = Double.POSITIVE_INFINITY;

        for (T n : nodes) {
            String key = keyFunction.getKey(n);
            int w = weightFunction.getWeight(n);
            if (w <= 0) {
                continue;
            }
            long h = hashFunction.hash64(objectKey + key);
            // Rendezvous/WRH：min(-ln(U)/w)
            double u = hashToUnitInterval(h);
            double score = -Math.log(u) / (double) w;

            if (score < bestScore) {
                bestScore = score;
                bestNode = n;
            }
        }
        return bestNode;
    }

    interface KeyFunction<T> {
        String getKey(T node);
    }

    interface WeightFunction<T> {
        int getWeight(T node);
    }

    interface HashFunction {
        long hash64(String key);
    }
}