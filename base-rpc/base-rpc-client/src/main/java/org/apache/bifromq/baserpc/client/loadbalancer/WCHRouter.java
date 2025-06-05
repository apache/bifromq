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
import java.util.SortedMap;
import java.util.TreeMap;

class WCHRouter<T> {
    private final SortedMap<Long, VirtualNode<T>> ring = new TreeMap<>();
    private final HashFunction hashFunction;
    private final KeyFunction<T> keyFunction;

    interface KeyFunction<T> {
        String getKey(T node);
    }

    interface WeightFunction<T> {
        int getWeight(T node);
    }

    interface HashFunction {
        long hash(String key);
    }

    static class VirtualNode<T> {
        final T physicalNode;
        final int replicaIndex;
        final KeyFunction<T> keyFunction;
        final String pNodeKey;
        final String vNodekey;

        VirtualNode(T physicalNode, int replicaIndex, KeyFunction<T> keyFunc) {
            this.replicaIndex = replicaIndex;
            this.physicalNode = physicalNode;
            this.keyFunction = keyFunc;
            pNodeKey = keyFunction.getKey(physicalNode);
            vNodekey = pNodeKey + "-" + replicaIndex;
        }

        public String getKey() {
            return vNodekey;
        }

        boolean isVirtualNodeOf(T pNode) {
            return pNodeKey.equals(keyFunction.getKey(pNode));
        }

        T getPhysicalNode() {
            return physicalNode;
        }
    }

    WCHRouter(Collection<T> pNodes,
              KeyFunction<T> keyFunction,
              WeightFunction<T> weightFunction,
              int vNodeCount) {
        this(pNodes, keyFunction, weightFunction, vNodeCount, new Murmur3());
    }

    WCHRouter(Collection<T> pNodes,
              KeyFunction<T> keyFunction,
              WeightFunction<T> weightFunction,
              int vNodeCount,
              HashFunction hashFunction) {
        this.keyFunction = keyFunction;
        this.hashFunction = hashFunction;
        if (pNodes != null) {
            for (T pNode : pNodes) {
                int weight = weightFunction.getWeight(pNode);
                addNode(pNode, vNodeCount * weight);
            }
        }
    }

    void addNode(T pNode, int nodeCount) {
        if (nodeCount < 0) {
            throw new IllegalArgumentException("illegal virtual node counts :" + nodeCount);
        }
        int existingReplicas = getExistingReplicas(pNode);
        for (int i = 0; i < nodeCount; i++) {
            VirtualNode<T> vNode = new VirtualNode<>(pNode, i + existingReplicas, keyFunction);
            ring.put(hashFunction.hash(vNode.getKey()), vNode);
        }
    }

    T routeNode(String objectKey) {
        if (ring.isEmpty()) {
            return null;
        }
        Long hashVal = hashFunction.hash(objectKey);
        SortedMap<Long, VirtualNode<T>> tailMap = ring.tailMap(hashVal);
        Long nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        return ring.get(nodeHashVal).getPhysicalNode();
    }

    boolean isEmpty() {
        return ring.isEmpty();
    }

    int getExistingReplicas(T pNode) {
        int replicas = 0;
        for (VirtualNode<T> vNode : ring.values()) {
            if (vNode.isVirtualNodeOf(pNode)) {
                replicas++;
            }
        }
        return replicas;
    }

    private static class Murmur3 implements HashFunction {
        com.google.common.hash.HashFunction hash;

        Murmur3() {
            hash = Hashing.murmur3_128();
        }

        @Override
        public long hash(String key) {
            HashCode code = hash.hashString(key, Charsets.UTF_8);
            return code.asLong();
        }
    }
}
