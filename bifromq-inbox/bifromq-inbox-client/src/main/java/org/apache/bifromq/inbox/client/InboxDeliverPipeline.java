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

package org.apache.bifromq.inbox.client;

import static org.apache.bifromq.inbox.util.InboxServiceUtil.parseTenantId;
import static org.apache.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_DELIVERERKEY;

import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import org.apache.bifromq.inbox.rpc.proto.SendReply;
import org.apache.bifromq.inbox.rpc.proto.SendRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.IDeliverer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxDeliverPipeline implements IDeliverer {
    private final IRPCClient.IRequestPipeline<SendRequest, SendReply> ppln;

    InboxDeliverPipeline(String delivererKey, IRPCClient rpcClient) {
        String tenantId = parseTenantId(delivererKey);
        ppln = rpcClient.createRequestPipeline(tenantId, null, delivererKey,
            Map.of(PIPELINE_ATTR_KEY_DELIVERERKEY, delivererKey), InboxServiceGrpc.getReceiveMethod());
    }

    @Override
    public CompletableFuture<DeliveryReply> deliver(DeliveryRequest request) {
        long reqId = System.nanoTime();
        return ppln.invoke(SendRequest.newBuilder()
                .setReqId(reqId)
                .setRequest(request)
                .build())
            .thenApply(SendReply::getReply)
            .exceptionally(e -> {
                log.debug("Failed to deliver request: {}", request, e);
                return DeliveryReply.newBuilder().setCode(DeliveryReply.Code.ERROR).build();
            });
    }

    @Override
    public void close() {
        ppln.close();
    }
}
