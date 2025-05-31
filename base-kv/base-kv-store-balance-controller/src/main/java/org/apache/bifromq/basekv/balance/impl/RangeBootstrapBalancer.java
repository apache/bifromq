/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basekv.balance.impl;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.DescriptorUtil.getEffectiveEpoch;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.NoNeedBalance;
import org.apache.bifromq.basekv.balance.StoreBalancer;
import org.apache.bifromq.basekv.balance.command.BootstrapCommand;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.utils.EffectiveEpoch;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * RangeBootstrapBalancer is a specialized StoreBalancer designed to handle the bootstrap process of creating the
 * initial key-value range in a distributed storage system. This balancer is responsible for initiating the creation of
 * the first full boundary range when no existing epochs are found in the cluster.
 */
public class RangeBootstrapBalancer extends StoreBalancer {
    private final Supplier<Long> millisSource;
    private final long suspicionDurationMillis;
    private final AtomicReference<BootstrapTrigger> bootstrapTrigger = new AtomicReference<>();
    /**
     * Constructor of StoreBalancer.
     *
     * @param clusterId    the id of the BaseKV cluster which the store belongs to
     * @param localStoreId the id of the store which the balancer is responsible for
     */
    public RangeBootstrapBalancer(String clusterId,
                                  String localStoreId) {
        this(clusterId, localStoreId, Duration.ofSeconds(15), HLC.INST::getPhysical);
    }

    /**
     * Constructor of the balancer with 15 seconds of suspicion duration.
     *
     * @param clusterId         the id of the BaseKV cluster which the store belongs to
     * @param localStoreId      the id of the store which the balancer is responsible for
     * @param suspicionDuration the duration of the replica being suspected unreachable
     */
    public RangeBootstrapBalancer(String clusterId, String localStoreId, Duration suspicionDuration) {
        this(clusterId, localStoreId, suspicionDuration, HLC.INST::getPhysical);
    }

    /**
     * Constructor of balancer.
     *
     * @param clusterId         the id of the BaseKV cluster which the store belongs to
     * @param localStoreId      the id of the store which the balancer is responsible for
     * @param suspicionDuration the duration of the replica being suspected unreachable
     * @param millisSource      the time source in milliseconds precision
     */
    RangeBootstrapBalancer(String clusterId,
                           String localStoreId,
                           Duration suspicionDuration,
                           Supplier<Long> millisSource) {
        super(clusterId, localStoreId);
        this.millisSource = millisSource;
        this.suspicionDurationMillis = suspicionDuration.toMillis();
    }

    @Override
    public void update(Set<KVRangeStoreDescriptor> landscape) {
        Optional<EffectiveEpoch> effectiveEpoch = getEffectiveEpoch(landscape);
        if (effectiveEpoch.isEmpty()) {
            if (bootstrapTrigger.get() == null) {
                KVRangeId rangeId = KVRangeIdUtil.generate();
                log.debug("No epoch found, schedule bootstrap command to create first full boundary range: {}",
                    KVRangeIdUtil.toString(rangeId));
                bootstrapTrigger.set(new BootstrapTrigger(rangeId, FULL_BOUNDARY, randomSuspicionTimeout()));
            }
        }
    }

    @Override
    public BalanceResult balance() {
        BootstrapTrigger current = bootstrapTrigger.get();
        if (current != null && millisSource.get() > current.triggerTime) {
            bootstrapTrigger.set(null);
            return BalanceNow.of(BootstrapCommand.builder()
                .toStore(localStoreId)
                .kvRangeId(current.id)
                .boundary(current.boundary)
                .build());
        }
        return NoNeedBalance.INSTANCE;
    }

    private long randomSuspicionTimeout() {
        return millisSource.get()
            + ThreadLocalRandom.current().nextLong(suspicionDurationMillis, suspicionDurationMillis * 2);
    }

    private record BootstrapTrigger(KVRangeId id, Boundary boundary, long triggerTime) {

    }
}
