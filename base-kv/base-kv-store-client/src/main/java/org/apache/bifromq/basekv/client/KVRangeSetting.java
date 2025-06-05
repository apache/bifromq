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

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@EqualsAndHashCode
@ToString
@Slf4j
public class KVRangeSetting {
    public final KVRangeId id;
    public final long ver;
    public final Boundary boundary;
    public final String leader;
    public final List<String> voters;
    public final List<String> followers;
    public final List<String> allReplicas;
    private final Any fact;
    private final String clusterId;
    private final List<String> inProcVoters;
    private final List<String> inProcFollowers;
    private final List<String> inProcReplicas;
    @EqualsAndHashCode.Exclude
    private volatile Object factObject;

    public KVRangeSetting(String clusterId, String leaderStoreId, KVRangeDescriptor desc) {
        this.clusterId = clusterId;
        id = desc.getId();
        ver = desc.getVer();
        boundary = desc.getBoundary();
        leader = leaderStoreId;
        fact = desc.getFact();
        Set<String> voters = new TreeSet<>();
        Set<String> inProcVoters = new TreeSet<>();
        Set<String> followers = new TreeSet<>();
        Set<String> inProcFollowers = new TreeSet<>();
        Set<String> allReplicas = new TreeSet<>();
        Set<String> inProcReplicas = new TreeSet<>();

        Set<String> allVoters =
            Sets.newHashSet(Iterables.concat(desc.getConfig().getVotersList(), desc.getConfig().getNextVotersList()));
        for (String v : allVoters) {
            if (desc.getSyncStateMap().get(v) == RaftNodeSyncState.Replicating) {
                voters.add(v);
                if (getInProcStores(clusterId).contains(v)) {
                    inProcVoters.add(v);
                }
                if (!v.equals(leaderStoreId)) {
                    followers.add(v);
                    if (getInProcStores(clusterId).contains(v)) {
                        inProcFollowers.add(v);
                    }
                }
                allReplicas.add(v);
                if (getInProcStores(clusterId).contains(v)) {
                    inProcReplicas.add(v);
                }
            }
        }
        Set<String> allLearners = Sets.union(Sets.newHashSet(
            Iterables.concat(desc.getConfig().getLearnersList(), desc.getConfig().getNextLearnersList())), allVoters);

        for (String v : allLearners) {
            if (desc.getSyncStateMap().get(v) == RaftNodeSyncState.Replicating) {
                allReplicas.add(v);
                if (getInProcStores(clusterId).contains(v)) {
                    inProcReplicas.add(v);
                }
            }
        }
        this.voters = Collections.unmodifiableList(Lists.newArrayList(voters));
        this.inProcVoters = Collections.unmodifiableList(Lists.newArrayList(inProcVoters));
        this.followers = Collections.unmodifiableList(Lists.newArrayList(followers));
        this.inProcFollowers = Collections.unmodifiableList(Lists.newArrayList(inProcFollowers));
        this.allReplicas = Collections.unmodifiableList(Lists.newArrayList(allReplicas));
        this.inProcReplicas = Collections.unmodifiableList(Lists.newArrayList(inProcReplicas));
    }

    public <T extends Message> Optional<T> getFact(Class<T> factType) {
        if (factObject == null) {
            synchronized (this) {
                if (factObject == null) {
                    try {
                        factObject = fact.unpack(factType);
                    } catch (InvalidProtocolBufferException e) {
                        log.error("parse fact error", e);
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.of((T) factObject);
    }

    public boolean hasInProcVoter() {
        return !inProcVoters.isEmpty();
    }

    public boolean hasInProcReplica() {
        return !inProcReplicas.isEmpty();
    }

    public String randomReplica() {
        if (!inProcReplicas.isEmpty()) {
            if (inProcReplicas.size() == 1) {
                return inProcReplicas.get(0);
            }
            return inProcReplicas.get(ThreadLocalRandom.current().nextInt(inProcReplicas.size()));
        }
        if (allReplicas.size() == 1) {
            return allReplicas.get(0);
        }
        return allReplicas.get(ThreadLocalRandom.current().nextInt(allReplicas.size()));
    }

    public String randomVoters() {
        if (getInProcStores(clusterId).contains(leader)) {
            return leader;
        } else if (!inProcVoters.isEmpty()) {
            if (inProcVoters.size() == 1) {
                return inProcVoters.get(0);
            }
            return inProcVoters.get(ThreadLocalRandom.current().nextInt(inProcVoters.size()));
        }
        if (voters.size() == 1) {
            return voters.get(0);
        }
        return voters.get(ThreadLocalRandom.current().nextInt(voters.size()));
    }
}
