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

package org.apache.bifromq.plugin.subbroker;

import java.util.concurrent.CompletableFuture;
import org.pf4j.ExtensionPoint;

/**
 * A sub broker is a downstream multi-tenant system which is capable of receiving subscribed messages in a batched way.
 */
public interface ISubBroker extends ExtensionPoint {
    /**
     * The id of the subscription broker.
     *
     * @return the statically assigned id for the downstream sub broker system
     */
    int id();

    /**
     * Check if the subscriptions exists.
     *
     * @param request subscriptions check request
     * @return a future of check results
     */
    CompletableFuture<CheckReply> check(CheckRequest request);

    /**
     * Open deliverer instance for delivering messages to the containing inboxes. It's guaranteed to have singleton
     * instance for each deliverer key.
     *
     * @param delivererKey the key of delivery group
     * @return the deliverer instance
     */
    IDeliverer open(String delivererKey);

    /**
     * Close the inbox broker.
     */
    void close();
}
