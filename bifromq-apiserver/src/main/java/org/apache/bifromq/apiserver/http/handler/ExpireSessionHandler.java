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

package org.apache.bifromq.apiserver.http.handler;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static org.apache.bifromq.apiserver.Headers.HEADER_EXPIRY_SECONDS;
import static org.apache.bifromq.apiserver.http.handler.utils.HeaderUtils.getHeader;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllReply;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllRequest;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;

@Path("/session")
final class ExpireSessionHandler extends TenantAwareHandler {
    private final IInboxClient inboxClient;

    ExpireSessionHandler(ISettingProvider settingProvider, IInboxClient inboxClient) {
        super(settingProvider);
        this.inboxClient = inboxClient;
    }

    @DELETE
    @Operation(summary = "Expire inactive persistent session using given expiry time")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER, description = "optional caller provided request id",
            schema = @Schema(implementation = Long.class)),
        @Parameter(name = "tenant_id", in = ParameterIn.HEADER, required = true, description = "the tenant id",
            schema = @Schema(implementation = String.class)),
        @Parameter(name = "expiry_seconds", in = ParameterIn.HEADER, required = true,
            description = "the overridden session expiry time in seconds",
            schema = @Schema(implementation = Integer.class)),
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success")
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        int expirySeconds = Integer.parseInt(getHeader(HEADER_EXPIRY_SECONDS, req, true));
        ExpireAllRequest.Builder reqBuilder = ExpireAllRequest.newBuilder()
            .setReqId(reqId)
            .setExpirySeconds(expirySeconds)
            .setNow(HLC.INST.getPhysical());
        if (tenantId != null) {
            reqBuilder.setTenantId(tenantId);
        }
        return inboxClient.expireAll(reqBuilder.build())
            .thenApply(r -> new DefaultFullHttpResponse(req.protocolVersion(),
                r.getCode() == ExpireAllReply.Code.OK ? OK : TOO_MANY_REQUESTS, Unpooled.EMPTY_BUFFER));
    }
}
