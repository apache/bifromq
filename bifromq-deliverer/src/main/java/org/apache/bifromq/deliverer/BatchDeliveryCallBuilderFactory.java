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

import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.IBatchCallBuilder;
import org.apache.bifromq.basescheduler.IBatchCallBuilderFactory;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.plugin.subbroker.IDeliverer;
import org.apache.bifromq.plugin.subbroker.ISubBrokerManager;

public class BatchDeliveryCallBuilderFactory
    implements IBatchCallBuilderFactory<DeliveryCall, DeliveryCallResult, DelivererKey> {
    private final IDistClient distClient;
    private final ISubBrokerManager subBrokerManager;

    public BatchDeliveryCallBuilderFactory(IDistClient distClient, ISubBrokerManager subBrokerManager) {
        this.distClient = distClient;
        this.subBrokerManager = subBrokerManager;
    }

    @Override
    public IBatchCallBuilder<DeliveryCall, DeliveryCallResult, DelivererKey> newBuilder(String name,
                                                                                        DelivererKey batcherKey) {
        int brokerId = batcherKey.subBrokerId();
        IDeliverer deliverer = subBrokerManager.get(brokerId).open(batcherKey.delivererKey());
        return new IBatchCallBuilder<>() {
            @Override
            public IBatchCall<DeliveryCall, DeliveryCallResult, DelivererKey> newBatchCall() {
                return new BatchDeliveryCall(distClient, deliverer, batcherKey);
            }

            @Override
            public void close() {
                deliverer.close();
            }
        };
    }
}
