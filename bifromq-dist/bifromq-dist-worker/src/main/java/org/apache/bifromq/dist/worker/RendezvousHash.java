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

package org.apache.bifromq.dist.worker;

import static com.google.common.hash.Hashing.murmur3_128;

import com.google.common.hash.Funnel;
import lombok.Builder;

/**
 * A simple implementation of Rendezvous Hashing.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the node.
 */
@Builder
class RendezvousHash<K, N> {
    private final Funnel<K> keyFunnel;
    private final Funnel<N> nodeFunnel;
    private final Iterable<N> nodes;

    /**
     * Get the node that the key should be mapped to.
     *
     * @param key The key.
     * @return The node.
     */
    public N get(K key) {
        long bestScore = Long.MIN_VALUE;
        N bestNode = null;
        for (N node : nodes) {
            long currentScore = murmur3_128()
                .newHasher()
                .putObject(key, keyFunnel)
                .putObject(node, nodeFunnel)
                .hash()
                .asLong();
            if (currentScore > bestScore) {
                bestScore = currentScore;
                bestNode = node;
            }
        }
        return bestNode;
    }
}