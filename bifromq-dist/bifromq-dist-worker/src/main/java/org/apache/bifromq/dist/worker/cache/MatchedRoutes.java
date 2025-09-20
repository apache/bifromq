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

package org.apache.bifromq.dist.worker.cache;

import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatching;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatching;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.distservice.GroupFanoutThrottled;
import org.apache.bifromq.plugin.eventcollector.distservice.PersistentFanoutThrottled;

public class MatchedRoutes implements IMatchedRoutes {
    private final String tenantId;
    private final String topic;
    private final IEventCollector eventCollector;
    private final Set<Matching> allMatchings = Sets.newConcurrentHashSet();
    private final Map<String, GroupMatching> groupMatchings = Maps.newConcurrentMap();
    private final AtomicInteger persistentFanout = new AtomicInteger();
    private int maxPersistentFanout;
    private int maxGroupFanout;

    public MatchedRoutes(String tenantId,
                         String topic,
                         IEventCollector eventCollector,
                         int maxPersistentFanout,
                         int maxGroupFanout) {
        this.tenantId = tenantId;
        this.topic = topic;
        this.eventCollector = eventCollector;
        this.maxPersistentFanout = maxPersistentFanout;
        this.maxGroupFanout = maxGroupFanout;
    }


    @Override
    public int maxPersistentFanout() {
        return maxPersistentFanout;
    }

    @Override
    public int maxGroupFanout() {
        return maxGroupFanout;
    }

    @Override
    public int persistentFanout() {
        return persistentFanout.get();
    }

    @Override
    public int groupFanout() {
        return groupMatchings.size();
    }

    @Override
    public Set<Matching> routes() {
        return allMatchings;
    }

    @Override
    public AddResult addNormalMatching(NormalMatching matching) {
        if (allMatchings.add(matching)) {
            if (matching.subBrokerId() == 1) {
                if (persistentFanout.get() < maxPersistentFanout) {
                    persistentFanout.incrementAndGet();
                    return AddResult.Added;
                } else {
                    allMatchings.remove(matching);
                    eventCollector.report(getLocal(PersistentFanoutThrottled.class)
                        .tenantId(tenantId)
                        .topic(topic)
                        .mqttTopicFilter(matching.mqttTopicFilter())
                        .maxCount(maxPersistentFanout)
                    );
                    return AddResult.ExceedFanoutLimit;
                }
            } else {
                return AddResult.Added;
            }
        }
        return AddResult.Exists;
    }

    @Override
    public void removeNormalMatching(NormalMatching matching) {
        if (allMatchings.remove(matching)) {
            if (matching.subBrokerId() == 1) {
                persistentFanout.decrementAndGet();
            }
        }
    }

    @Override
    public AddResult putGroupMatching(GroupMatching matching) {
        GroupMatching prev = groupMatchings.put(matching.mqttTopicFilter(), matching);
        if (prev == null) {
            if (groupMatchings.size() <= maxGroupFanout) {
                allMatchings.add(matching);
                return AddResult.Added;
            } else {
                groupMatchings.remove(matching.mqttTopicFilter());
                eventCollector.report(getLocal(GroupFanoutThrottled.class)
                    .tenantId(tenantId)
                    .topic(topic)
                    .mqttTopicFilter(matching.mqttTopicFilter())
                    .maxCount(maxGroupFanout)
                );
                return AddResult.ExceedFanoutLimit;
            }
        } else {
            allMatchings.remove(prev);
            allMatchings.add(matching);
            return AddResult.Exists;
        }
    }

    @Override
    public void removeGroupMatching(GroupMatching matching) {
        assert matching.receivers().isEmpty();
        removeGroupMatching(matching.mqttTopicFilter());
    }

    private void removeGroupMatching(String mqttTopicFilter) {
        GroupMatching existing = groupMatchings.remove(mqttTopicFilter);
        if (existing != null) {
            allMatchings.remove(existing);
        }
    }

    @Override
    public AdjustResult adjust(int newMaxPersistentFanout, int newMaxGroupFanout) {
        // current limit increases and cachedRoutes reached the previous limit, need refresh
        if (maxPersistentFanout < newMaxPersistentFanout && persistentFanout.get() == maxPersistentFanout) {
            return AdjustResult.ReloadNeeded;
        }
        if (maxGroupFanout < newMaxGroupFanout && groupMatchings.size() == maxGroupFanout) {
            return AdjustResult.ReloadNeeded;
        }
        // current limit decreases and cachedRoutes exceeded the current limit, need refresh
        boolean clamped = false;
        if (maxPersistentFanout > newMaxPersistentFanout && persistentFanout.get() > newMaxPersistentFanout) {
            // clamp persistent fanout
            int toRemove = persistentFanout.get() - newMaxPersistentFanout;
            List<NormalMatching> toRemoved = new ArrayList<>(toRemove);
            for (Matching matching : allMatchings) {
                if (toRemove == 0) {
                    break;
                }
                if (matching.type() == Matching.Type.Normal && ((NormalMatching) matching).subBrokerId() == 1) {
                    toRemoved.add((NormalMatching) matching);
                    toRemove--;
                }
            }
            toRemoved.forEach(this::removeNormalMatching);
            clamped = true;
        }
        if (maxGroupFanout > newMaxGroupFanout && groupMatchings.size() > newMaxGroupFanout) {
            // clamp persistent fanout
            int toRemove = groupMatchings.size() - newMaxGroupFanout;
            List<String> toRemoved = new ArrayList<>(toRemove);
            for (String mqttTopicFilter : groupMatchings.keySet()) {
                if (toRemove == 0) {
                    break;
                }
                toRemoved.add(mqttTopicFilter);
                toRemove--;
            }
            toRemoved.forEach(this::removeGroupMatching);
            clamped = true;
        }
        maxPersistentFanout = newMaxPersistentFanout;
        maxGroupFanout = newMaxGroupFanout;
        if (clamped) {
            return AdjustResult.Clamped;
        }
        return AdjustResult.Adjusted;
    }
}
