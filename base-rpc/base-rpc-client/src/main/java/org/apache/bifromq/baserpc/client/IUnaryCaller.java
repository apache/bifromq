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

package org.apache.bifromq.baserpc.client;

import static java.util.Collections.emptyMap;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The caller for unary RPC.
 *
 * @param <ReqT>  the request type
 * @param <RespT> the response type
 */
interface IUnaryCaller<ReqT, RespT> {
    /**
     * Invoke a request to the desired server.
     *
     * @param tenantId         the tenant id
     * @param targetServerId   the target server id, can be null
     * @param req              the request to send
     * @return a CompletableFuture of the response
     */
    default CompletableFuture<RespT> invoke(String tenantId, String targetServerId, ReqT req) {
        return invoke(tenantId, targetServerId, req, emptyMap());
    }

    /**
     * Invoke a request to the desired server with metadata.
     *
     * @param tenantId the tenant id
     * @param targetServerId the target server id, can be null
     * @param req the request to send
     * @param metadata additional metadata to attach to the request
     * @return a CompletableFuture of the response
     */
    CompletableFuture<RespT> invoke(String tenantId,
                                    String targetServerId,
                                    ReqT req,
                                    Map<String, String> metadata);
}
