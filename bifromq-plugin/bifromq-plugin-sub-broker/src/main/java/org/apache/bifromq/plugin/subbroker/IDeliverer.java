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

/**
 * Deliverer is responsible for delivering messages to the subscribers.
 */
public interface IDeliverer {

    /**
     * Deliver a pack of messages into the subscriber inboxes.
     *
     * @param request a pack of messages to be delivered
     * @return a future of reply
     */
    CompletableFuture<DeliveryReply> deliver(DeliveryRequest request);

    /**
     * This method will be called whenever it's determined in IDLE state, release any resources associated if any.
     */
    void close();
}
