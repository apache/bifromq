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

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

class WeightedServerGroupRouter implements IServerGroupRouter {
    private final Map<String, Integer> weightedServers;
    private final List<String> weightedServerRRSequence;
    private final WCHRouter<String> chRouter;
    private final AtomicInteger rrIndex = new AtomicInteger(0);
    private final Set<String> inProcServers = new HashSet<>();

    WeightedServerGroupRouter(Map<String, Boolean> allServers,
                              Map<String, Integer> groupWeights,
                              Map<String, Set<String>> serverGroups) {
        weightedServers = Maps.newHashMap();
        for (String group : groupWeights.keySet()) {
            int weight = Math.abs(groupWeights.get(group)) % 11; // weight range: 0-10
            serverGroups.getOrDefault(group, Collections.emptySet()).forEach(serverId ->
                weightedServers.compute(serverId, (k, w) -> {
                    if (w == null) {
                        w = 0;
                    }
                    w = Math.max(w, weight);
                    return w;
                }));
        }
        weightedServerRRSequence = LBUtils.toWeightedRRSequence(weightedServers);
        chRouter = new WCHRouter<>(weightedServers.keySet(), serverId -> serverId, weightedServers::get, 100);
        // if inproc server is not in the weightedServers, it will be ignored
        for (String serverId : weightedServers.keySet()) {
            if (allServers.getOrDefault(serverId, false)) {
                inProcServers.add(serverId);
            }
        }
    }

    @Override
    public boolean isSameGroup(IServerGroupRouter other) {
        if (other instanceof WeightedServerGroupRouter otherRouter) {
            return weightedServers.equals(otherRouter.weightedServers);
        }
        return false;
    }

    @Override
    public Optional<String> random() {
        if (weightedServerRRSequence.isEmpty()) {
            return Optional.empty();
        }
        // prefer in-proc server
        if (!inProcServers.isEmpty()) {
            return inProcServers.stream().findFirst();
        }
        return Optional.of(
            weightedServerRRSequence.get(ThreadLocalRandom.current().nextInt(0, weightedServerRRSequence.size())));
    }

    @Override
    public Optional<String> roundRobin() {
        int size = weightedServerRRSequence.size();
        if (size == 0) {
            return Optional.empty();
        }
        // prefer in-proc server
        if (!inProcServers.isEmpty()) {
            return inProcServers.stream().findFirst();
        } else {
            int i = rrIndex.incrementAndGet();
            if (i >= size) {
                int oldi = i;
                i %= size;
                rrIndex.compareAndSet(oldi, i);
            }
            return Optional.of(weightedServerRRSequence.get(i));
        }
    }

    @Override
    public Optional<String> tryRoundRobin() {
        int size = weightedServerRRSequence.size();
        if (size == 0) {
            return Optional.empty();
        }
        // prefer in-proc server
        if (!inProcServers.isEmpty()) {
            return inProcServers.stream().findFirst();
        } else {
            int i = rrIndex.get() + 1;
            if (i >= size) {
                i %= size;
            }
            return Optional.of(weightedServerRRSequence.get(i));
        }
    }

    @Override
    public Optional<String> hashing(String key) {
        return Optional.ofNullable(chRouter.routeNode(key));
    }
}
