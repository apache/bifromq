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

package org.apache.bifromq.inbox.server;

import static org.apache.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_DELIVERERKEY;

import org.apache.bifromq.baseenv.MemUsage;
import org.apache.bifromq.baserpc.server.ResponsePipeline;
import org.apache.bifromq.inbox.record.TenantInboxInstance;
import org.apache.bifromq.inbox.rpc.proto.SendReply;
import org.apache.bifromq.inbox.rpc.proto.SendRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.sysprops.props.IngressSlowDownDirectMemoryUsage;
import org.apache.bifromq.sysprops.props.IngressSlowDownHeapMemoryUsage;
import org.apache.bifromq.sysprops.props.MaxSlowDownTimeoutSeconds;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxWriterPipeline extends ResponsePipeline<SendRequest, SendReply> {
    private static final double SLOWDOWN_DIRECT_MEM_USAGE = IngressSlowDownDirectMemoryUsage.INSTANCE.get();
    private static final double SLOWDOWN_HEAP_MEM_USAGE = IngressSlowDownHeapMemoryUsage.INSTANCE.get();
    private static final Duration SLOWDOWN_TIMEOUT = Duration.ofSeconds(MaxSlowDownTimeoutSeconds.INSTANCE.get());
    private final IWriteCallback writeCallback;
    private final ISendRequestHandler handler;
    private final String delivererKey;

    public InboxWriterPipeline(IWriteCallback writeCallback,
                               ISendRequestHandler handler,
                               StreamObserver<SendReply> responseObserver) {
        super(responseObserver, () -> MemUsage.local().nettyDirectMemoryUsage() > SLOWDOWN_DIRECT_MEM_USAGE
            || MemUsage.local().heapMemoryUsage() > SLOWDOWN_HEAP_MEM_USAGE, SLOWDOWN_TIMEOUT);
        this.writeCallback = writeCallback;
        this.handler = handler;
        this.delivererKey = metadata(PIPELINE_ATTR_KEY_DELIVERERKEY);
    }

    @Override
    protected CompletableFuture<SendReply> handleRequest(String ignore, SendRequest request) {
        log.trace("Received inbox write request: deliverer={}, \n{}", delivererKey, request);
        return handler.handle(request)
            .thenApply(v -> {
                v.getReply().getResultMap()
                    .forEach((tenantId, deliveryResults) ->
                        deliveryResults.getResultList()
                            .forEach(result -> {
                                if (result.getCode() == DeliveryResult.Code.OK) {
                                    writeCallback.afterWrite(TenantInboxInstance.from(tenantId, result.getMatchInfo()),
                                        delivererKey);
                                }
                            }));
                return v;
            });
    }

    interface IWriteCallback {
        void afterWrite(TenantInboxInstance tenantInboxInstance, String delivererKey);
    }

    interface ISendRequestHandler {
        CompletableFuture<SendReply> handle(SendRequest request);
    }
}
