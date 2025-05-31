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

package org.apache.bifromq.retain.store.balance;

import org.apache.bifromq.basekv.balance.StoreBalancer;
import org.apache.bifromq.basekv.balance.impl.ReplicaCntBalancer;
import org.apache.bifromq.retain.store.spi.IRetainStoreBalancerFactory;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReplicaCntBalancerFactory implements IRetainStoreBalancerFactory {
    private static final String VOTERS_PER_RANGE = "votersPerRange";
    private static final String LEARNERS_PER_RANGE = "learnersPerRange";
    private static final int DEFAULT_VOTERS_PER_RANGE = 3;
    private static final int DEFAULT_LEARNERS_PER_RANGE = -1;
    private int votersPerRange;
    private int learnersPerRange;

    @Override
    public void init(Struct config) {
        votersPerRange = (int) config.getFieldsOrDefault(VOTERS_PER_RANGE,
            Value.newBuilder().setNumberValue(DEFAULT_VOTERS_PER_RANGE).build()).getNumberValue();
        if (votersPerRange < 1 || votersPerRange % 2 == 0) {
            votersPerRange = 3;
            log.warn("Invalid voters per range config {}, use default {}", votersPerRange, DEFAULT_VOTERS_PER_RANGE);
        }
        learnersPerRange = (int) config.getFieldsOrDefault(LEARNERS_PER_RANGE,
            Value.newBuilder().setNumberValue(DEFAULT_LEARNERS_PER_RANGE).build()).getNumberValue();
        if (learnersPerRange < -1) {
            learnersPerRange = 0;
            log.warn("Invalid learners per range config {}, use default {}", learnersPerRange,
                DEFAULT_LEARNERS_PER_RANGE);
        }
    }

    @Override
    public StoreBalancer newBalancer(String clusterId, String localStoreId) {
        return new ReplicaCntBalancer(clusterId, localStoreId, votersPerRange, learnersPerRange);
    }
}
