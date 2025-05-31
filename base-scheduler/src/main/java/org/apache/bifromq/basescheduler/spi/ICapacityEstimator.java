/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basescheduler.spi;

/**
 * SPI interface for estimating the capacity of a BatchCall pipeline.
 */
public interface ICapacityEstimator {
    /**
     * Callback to record the latency of a batch call.
     *
     * @param batchSize the size of the batch
     * @param latencyNs the latency in nanoseconds
     */
    void record(int batchSize, long latencyNs);

    /**
     * Get the maximum pipeline depth for the BatchCall pipeline.
     *
     * @return the maximum pipeline depth
     */
    int maxPipelineDepth();

    /**
     * Get the maximum batch size for a batch.
     *
     * @return the maximum batch size
     */
    int maxBatchSize();

    /**
     * Close the call scheduler.
     */
    default void close() {
    }
}
