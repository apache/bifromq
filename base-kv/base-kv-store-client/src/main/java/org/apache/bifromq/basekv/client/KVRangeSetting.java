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

package org.apache.bifromq.basekv.client;

import static org.apache.bifromq.basekv.InProcStores.getInProcStores;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;

@EqualsAndHashCode(cacheStrategy = EqualsAndHashCode.CacheStrategy.LAZY)
@ToString
@Accessors(fluent = true)
@Slf4j
public class KVRangeSetting {
    private final Any fact;
    private final String clusterId;
    @Getter
    private final KVRangeId id;
    @Getter
    private final long ver;
    @Getter
    private final Boundary boundary;
    @Getter
    private final String leader;
    private final List<String> allQueryReadyReplicas;
    private final List<String> inProcReplicas;
    @EqualsAndHashCode.Exclude
    private volatile Object factObject;

    public KVRangeSetting(String clusterId, String leaderStoreId, Map<String, KVRangeDescriptor> currentReplicas) {
        assert currentReplicas.containsKey(leaderStoreId) : "replicas doesn't contain leader store";
        KVRangeDescriptor leaderDesc = currentReplicas.get(leaderStoreId);
        this.clusterId = clusterId;
        id = leaderDesc.getId();
        ver = leaderDesc.getVer();
        boundary = leaderDesc.getBoundary();
        leader = leaderStoreId;
        fact = leaderDesc.getFact();
        Set<String> allQueryReadyReplicas = new TreeSet<>();
        Set<String> inProcReplicas = new TreeSet<>();

        Set<String> allVoters = Sets.newHashSet(
            Iterables.concat(leaderDesc.getConfig().getVotersList(), leaderDesc.getConfig().getNextVotersList()));
        for (String v : allVoters) {
            if (leaderDesc.getSyncStateMap().get(v) == RaftNodeSyncState.Replicating) {
                if (isReadyForQuery(v, currentReplicas)) {
                    allQueryReadyReplicas.add(v);
                }
                if (getInProcStores(clusterId).contains(v)) {
                    inProcReplicas.add(v);
                }
            }
        }
        Set<String> allLearners = Sets.union(Sets.newHashSet(
                Iterables.concat(leaderDesc.getConfig().getLearnersList(), leaderDesc.getConfig().getNextLearnersList())),
            allVoters);

        for (String l : allLearners) {
            if (leaderDesc.getSyncStateMap().get(l) == RaftNodeSyncState.Replicating) {
                if (isReadyForQuery(l, currentReplicas)) {
                    allQueryReadyReplicas.add(l);
                }
                if (getInProcStores(clusterId).contains(l)) {
                    inProcReplicas.add(l);
                }
            }
        }
        this.allQueryReadyReplicas = Collections.unmodifiableList(Lists.newArrayList(allQueryReadyReplicas));
        this.inProcReplicas = Collections.unmodifiableList(Lists.newArrayList(inProcReplicas));
    }

    public <T extends Message> Optional<T> getFact(Class<T> factType) {
        if (factObject == null) {
            synchronized (this) {
                if (factObject == null) {
                    try {
                        // Only unpack when the embedded type matches; otherwise treat as absent
                        if (fact.is(factType)) {
                            factObject = fact.unpack(factType);
                        } else {
                            return Optional.empty();
                        }
                    } catch (InvalidProtocolBufferException e) {
                        log.error("parse fact error", e);
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.of((T) factObject);
    }

    public Optional<String> inProcQueryReadyReplica() {
        if (!inProcReplicas.isEmpty()) {
            if (inProcReplicas.size() == 1) {
                if (allQueryReadyReplicas.contains(inProcReplicas.get(0))) {
                    return Optional.of(inProcReplicas.get(0));
                }
            }
            return allQueryReadyReplicas.stream().filter(inProcReplicas::contains).findFirst();
        }
        return Optional.empty();
    }

    public Optional<String> getQueryReadyReplica(int seq) {
        if (allQueryReadyReplicas.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(allQueryReadyReplicas.get(seq % allQueryReadyReplicas.size()));
    }

    public Optional<String> randomReplicaForQuery() {
        if (!inProcReplicas.isEmpty()) {
            if (inProcReplicas.size() == 1 && allQueryReadyReplicas.contains(inProcReplicas.get(0))) {
                return Optional.of(inProcReplicas.get(0));
            }
        }
        if (allQueryReadyReplicas.isEmpty()) {
            return Optional.empty();
        }
        if (allQueryReadyReplicas.size() == 1) {
            return Optional.of(allQueryReadyReplicas.get(0));
        }
        return Optional.of(
            allQueryReadyReplicas.get(ThreadLocalRandom.current().nextInt(allQueryReadyReplicas.size())));
    }

    private boolean isReadyForQuery(String storeId, Map<String, KVRangeDescriptor> descMap) {
        KVRangeDescriptor desc = descMap.get(storeId);
        if (desc == null) {
            return false;
        }
        return desc.getReadyForQuery();
    }
}
