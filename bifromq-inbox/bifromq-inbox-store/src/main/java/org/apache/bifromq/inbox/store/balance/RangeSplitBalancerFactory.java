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

package org.apache.bifromq.inbox.store.balance;


import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.balance.StoreBalancer;
import org.apache.bifromq.basekv.balance.impl.RangeSplitBalancer;
import org.apache.bifromq.inbox.store.spi.IInboxStoreBalancerFactory;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RangeSplitBalancerFactory implements IInboxStoreBalancerFactory {
    private static final String MAX_RANGES_PER_STORE = "maxRangesPerStore";
    private static final String MAX_CPU_USAGE = "maxCPUUsage";
    private static final String MAX_IO_DENSITY = "maxIODensity";
    private static final String IO_NANOS_LIMITS = "ioNanosLimit";
    private static final int DEFAULT_MAX_RANGES_PER_STORE = EnvProvider.INSTANCE.availableProcessors() / 4;
    private static final double DEFAULT_MAX_CPU_USAGE = 0.8;
    private static final int DEFAULT_MAX_IO_DENSITY = 100;
    private static final long DEFAULT_IO_NANOS_LIMITS = 30_000L;

    private int maxRangesPerStore;
    private double maxCPUUsage;
    private int maxIODensity;
    private long ioNanosLimits;

    @Override
    public void init(Struct config) {
        maxRangesPerStore = (int) config.getFieldsOrDefault(MAX_RANGES_PER_STORE,
            Value.newBuilder().setNumberValue(DEFAULT_MAX_RANGES_PER_STORE).build()).getNumberValue();
        if (maxRangesPerStore < 1) {
            maxRangesPerStore = DEFAULT_MAX_RANGES_PER_STORE;
            log.warn("Invalid max ranges per store config {}, use default {}", maxRangesPerStore,
                DEFAULT_MAX_RANGES_PER_STORE);
        }
        maxCPUUsage = config.getFieldsOrDefault(MAX_CPU_USAGE,
            Value.newBuilder().setNumberValue(DEFAULT_MAX_CPU_USAGE).build()).getNumberValue();
        if (maxCPUUsage < 0 || maxCPUUsage > 1) {
            maxCPUUsage = DEFAULT_MAX_CPU_USAGE;
            log.warn("Invalid max cpu usage config {}, use default {}", maxCPUUsage, DEFAULT_MAX_CPU_USAGE);
        }
        maxIODensity = (int) config.getFieldsOrDefault(MAX_IO_DENSITY,
            Value.newBuilder().setNumberValue(DEFAULT_MAX_IO_DENSITY).build()).getNumberValue();
        if (maxIODensity < 1 || maxIODensity > 1000) {
            maxIODensity = DEFAULT_MAX_IO_DENSITY;
            log.warn("Invalid max io density config {}, use default {}", maxIODensity, DEFAULT_MAX_IO_DENSITY);
        }
        ioNanosLimits = (long) config.getFieldsOrDefault(IO_NANOS_LIMITS,
            Value.newBuilder().setNumberValue(DEFAULT_IO_NANOS_LIMITS).build()).getNumberValue();
        if (ioNanosLimits < 1 || ioNanosLimits > 100_000) {
            ioNanosLimits = DEFAULT_IO_NANOS_LIMITS;
            log.warn("Invalid io nanos limits config {}, use default {}", ioNanosLimits, DEFAULT_IO_NANOS_LIMITS);
        }
    }

    @Override
    public StoreBalancer newBalancer(String clusterId, String localStoreId) {
        return new RangeSplitBalancer(clusterId, localStoreId, "kv_io_mutation",
            maxRangesPerStore, maxCPUUsage, maxIODensity, ioNanosLimits);
    }
}
