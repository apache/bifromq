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

package org.apache.bifromq.basescheduler;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimator;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimatorFactory;

@Slf4j
class CapacityEstimatorFactory implements ICapacityEstimatorFactory {
    public static final ICapacityEstimatorFactory INSTANCE = new CapacityEstimatorFactory();

    private final ICapacityEstimatorFactory delegate;

    private CapacityEstimatorFactory() {
        Map<String, ICapacityEstimatorFactory> factoryMap = BaseHookLoader.load(ICapacityEstimatorFactory.class);
        if (factoryMap.isEmpty()) {
            delegate = FallbackFactory.INSTANCE;
        } else {
            delegate = factoryMap.values().iterator().next();
            if (factoryMap.size() > 1) {
                log.warn("Multiple CapacityEstimatorFactory implementations found, the first loaded will be used:{}",
                    delegate.getClass().getName());
            }
        }
    }

    @Override
    public <BatcherKey> ICapacityEstimator<BatcherKey> get(String name, BatcherKey batcherKey) {
        try {
            ICapacityEstimator<BatcherKey> estimator = delegate.get(name, batcherKey);
            if (estimator == null) {
                return FallbackFactory.INSTANCE.get(name, batcherKey);
            }
            return estimator;
        } catch (Throwable e) {
            log.error("Failed to create CapacityEstimator: scheduler={}", name, e);
            return FallbackFactory.INSTANCE.get(name, batcherKey);
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static class FallbackCapacityEstimator<BatcherKey> implements ICapacityEstimator<BatcherKey> {

        @Override
        public void record(long weightedSize, long latencyNs) {
        }

        public boolean hasCapacity(long inflight, BatcherKey key) {
            return inflight <= 0;
        }

        @Override
        public long maxCapacity(BatcherKey key) {
            return Long.MAX_VALUE;
        }

        @Override
        public void onBackPressure() {
        }
    }

    private static class FallbackFactory implements ICapacityEstimatorFactory {
        private static final ICapacityEstimatorFactory INSTANCE = new FallbackFactory();

        @Override
        public <BatcherKey> ICapacityEstimator<BatcherKey> get(String name, BatcherKey batcherKey) {
            return new FallbackCapacityEstimator<>();
        }
    }
}
