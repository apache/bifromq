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

import org.apache.bifromq.basehookloader.BaseHookLoader;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimator;
import org.apache.bifromq.basescheduler.spi.ICapacityEstimatorFactory;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

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
        }
    }

    @Override
    public ICapacityEstimator create(String name) {
        try {
            ICapacityEstimator estimator = delegate.create(name);
            if (estimator == null) {
                return FallbackFactory.INSTANCE.create(name);
            }
            return estimator;
        } catch (Throwable e) {
            log.error("Failed to create pipelineDepthEstimator: scheduler={}", name, e);
            return FallbackFactory.INSTANCE.create(name);
        }
    }

    private static class FallbackCapacityEstimator implements ICapacityEstimator {
        private static final ICapacityEstimator INSTANCE = new FallbackCapacityEstimator();

        @Override
        public void record(int batchSize, long latencyNs) {
        }

        @Override
        public int maxPipelineDepth() {
            return 1;
        }

        @Override
        public int maxBatchSize() {
            return Integer.MAX_VALUE;
        }
    }

    private static class FallbackFactory implements ICapacityEstimatorFactory {
        private static final ICapacityEstimatorFactory INSTANCE = new FallbackFactory();

        @Override
        public ICapacityEstimator create(String name) {
            return FallbackCapacityEstimator.INSTANCE;
        }
    }
}
