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

package org.apache.bifromq.dist.client;

import org.apache.bifromq.baserpc.client.IConnectable;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.RouteMatcher;
import java.util.concurrent.CompletableFuture;

/**
 * The dist client interface.
 */
public interface IDistClient extends IConnectable, AutoCloseable {
    static DistClientBuilder newBuilder() {
        return new DistClientBuilder();
    }

    /**
     * publish a message at best effort, there are many edge cases that could lead to publish failure, so the normal
     * completion of the returned future doesn't mean the message has been delivered successfully. There are various
     * events reported during the process.
     *
     * @param reqId     the caller supplied request id for event tracing
     * @param topic     the message topic
     * @param publisher the publisher of the message which must have non-null tenantId and type field
     * @return a future for tracking the publishing process asynchronously
     */
    CompletableFuture<PubResult> pub(long reqId, String topic, Message message, ClientInfo publisher);

    /**
     * Add a topic match.
     *
     * @param reqId        the caller supplied request id for event tracing
     * @param tenantId     the id of caller tenant
     * @param matcher      the route matcher
     * @param receiverId   the id of the receiverInfo hosted by the subbroker
     * @param delivererKey the key of the deliverer via which the message will be sent to the receiverInfo
     * @param subBrokerId  the hosting subbroker of the receiverInfo
     * @param incarnation  the incarnation of the receiverInfo
     * @return correspond to Mqtt Sub QoS
     */
    CompletableFuture<MatchResult> addRoute(long reqId,
                                            String tenantId,
                                            RouteMatcher matcher,
                                            String receiverId,
                                            String delivererKey,
                                            int subBrokerId,
                                            long incarnation);

    /**
     * Remove a topic match.
     *
     * @param reqId        the caller supplied request id for event tracing
     * @param tenantId     the id of caller tenant
     * @param matcher      the topic filter to remove
     * @param receiverId   the id of the receiverInfo hosted by the subbroker
     * @param delivererKey the key of the deliverer via which the message will be sent to the receiverInfo
     * @param subBrokerId  the hosting subbroker of the receiverInfo
     * @param incarnation  the incarnation of the receiverInfo
     * @return true for remove successfully, false for match not found
     */
    CompletableFuture<UnmatchResult> removeRoute(long reqId,
                                                 String tenantId,
                                                 RouteMatcher matcher,
                                                 String receiverId,
                                                 String delivererKey,
                                                 int subBrokerId,
                                                 long incarnation);

    void close();
}
