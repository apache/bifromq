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

package org.apache.bifromq.sessiondict.server;

import org.apache.bifromq.baserpc.server.ResponsePipeline;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.sessiondict.rpc.proto.ExistReply;
import org.apache.bifromq.sessiondict.rpc.proto.ExistRequest;
import org.apache.bifromq.type.ClientInfo;

class SessionExistPipeline extends ResponsePipeline<ExistRequest, ExistReply> {
    private final ISessionRegistry sessionRegistry;

    public SessionExistPipeline(ISessionRegistry sessionRegistry, StreamObserver<ExistReply> responseObserver) {
        super(responseObserver);
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    protected CompletableFuture<ExistReply> handleRequest(String tenantId, ExistRequest request) {
        ExistReply.Builder respBuilder = ExistReply.newBuilder()
            .setReqId(request.getReqId())
            .setCode(ExistReply.Code.OK);
        for (ExistRequest.Client client : request.getClientList()) {
            Optional<ClientInfo> clientOpt = sessionRegistry.get(tenantId, client.getUserId(), client.getClientId());
            respBuilder.addExist(clientOpt.isPresent());
        }
        return CompletableFuture.completedFuture(respBuilder.build());
    }
}
