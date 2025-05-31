/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.mqtt.service;

import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.type.MatchInfo;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Local topic router maintains the mapping between topic filter and channel id which made by non-shared subscription.
 * Internally the mappings are organized into buckets, each bucket identified by its id as a receiverInfo could be routed
 * globally by dist service.
 */
public interface ILocalTopicRouter {
    CompletableFuture<MatchResult> addTopicRoute(long reqId,
                                                 String tenantId,
                                                 String topicFilter,
                                                 long incarnation,
                                                 String channelId);

    CompletableFuture<UnmatchResult> removeTopicRoute(long reqId,
                                                      String tenantId,
                                                      String topicFilter,
                                                      long incarnation,
                                                      String channelId);

    Optional<CompletableFuture<? extends ILocalRoutes>> getTopicRoutes(String tenantId, MatchInfo matchInfo);

    /**
     * The bucketed local routes for a topic filter.
     */
    interface ILocalRoutes {
        String localReceiverId();

        Map<String, Long> routesInfo();

        long incarnation();
    }
}
