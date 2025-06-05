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

package org.apache.bifromq.deliverer;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import org.apache.bifromq.basescheduler.BatchCallScheduler;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.sysprops.props.DataPlaneMaxBurstLatencyMillis;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageDeliverer extends BatchCallScheduler<DeliveryCall, DeliveryCallResult, DelivererKey>
    implements IMessageDeliverer {

    public MessageDeliverer(BatchDeliveryCallBuilderFactory batchDeliveryCallBuilderFactory) {
        super(batchDeliveryCallBuilderFactory,
            Duration.ofMillis(DataPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos());
    }

    @Override
    public CompletableFuture<DeliveryCallResult> schedule(DeliveryCall request) {
        return super.schedule(request).exceptionally(unwrap(e -> {
            if (e instanceof BackPressureException) {
                return DeliveryCallResult.BACK_PRESSURE_REJECTED;
            }
            log.error("Failed to schedule delivery call", e);
            return DeliveryCallResult.ERROR;
        }));
    }

    @Override
    protected Optional<DelivererKey> find(DeliveryCall request) {
        return Optional.of(request.delivererKey);
    }
}
