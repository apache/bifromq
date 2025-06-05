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

package org.apache.bifromq.mqtt.service;

import org.apache.bifromq.baserpc.server.ResponsePipeline;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baseenv.MemUsage;
import org.apache.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import org.apache.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import org.apache.bifromq.sysprops.props.IngressSlowDownDirectMemoryUsage;
import org.apache.bifromq.sysprops.props.IngressSlowDownHeapMemoryUsage;
import org.apache.bifromq.sysprops.props.MaxSlowDownTimeoutSeconds;

@Slf4j
class LocalSessionWritePipeline extends ResponsePipeline<WriteRequest, WriteReply> {
    private static final double SLOWDOWN_DIRECT_MEM_USAGE = IngressSlowDownDirectMemoryUsage.INSTANCE.get();
    private static final double SLOWDOWN_HEAP_MEM_USAGE = IngressSlowDownHeapMemoryUsage.INSTANCE.get();
    private static final Duration SLOWDOWN_TIMEOUT = Duration.ofSeconds(MaxSlowDownTimeoutSeconds.INSTANCE.get());
    private final ILocalDistService localDistService;

    public LocalSessionWritePipeline(ILocalDistService localDistService, StreamObserver<WriteReply> responseObserver) {
        super(responseObserver, () -> MemUsage.local().nettyDirectMemoryUsage() > SLOWDOWN_DIRECT_MEM_USAGE
            || MemUsage.local().heapMemoryUsage() > SLOWDOWN_HEAP_MEM_USAGE, SLOWDOWN_TIMEOUT);
        this.localDistService = localDistService;
    }

    @Override
    protected CompletableFuture<WriteReply> handleRequest(String tenantId, WriteRequest request) {
        log.trace("Handle inbox write request: \n{}", request);
        return localDistService.dist(request.getRequest())
            .thenApply(reply -> WriteReply.newBuilder().setReqId(request.getReqId()).setReply(reply).build());
    }
}
