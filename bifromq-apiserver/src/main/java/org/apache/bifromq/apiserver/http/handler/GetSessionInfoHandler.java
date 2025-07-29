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

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_ID;
import static org.apache.bifromq.apiserver.Headers.HEADER_USER_ID;
import static org.apache.bifromq.apiserver.http.handler.utils.HeaderUtils.getHeader;

import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.rpc.proto.GetRequest;
import org.apache.bifromq.type.ClientInfo;

@Path("/session")
final class GetSessionInfoHandler extends TenantAwareHandler {
    private final ISessionDictClient sessionDictClient;

    GetSessionInfoHandler(ISettingProvider settingProvider, ISessionDictClient sessionDictClient) {
        super(settingProvider);
        this.sessionDictClient = sessionDictClient;
    }

    @GET
    @Operation(summary = "Get the session information of the given user and client id")

    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "tenant_id", in = ParameterIn.HEADER, required = true,
            description = "the id of tenant", schema = @Schema(implementation = String.class)),
        @Parameter(name = "user_id", in = ParameterIn.HEADER, required = true,
            description = "the id of user who established the session",
            schema = @Schema(implementation = String.class)),
        @Parameter(name = "client_id", in = ParameterIn.HEADER, required = true,
            description = "the client id of the mqtt session", schema = @Schema(implementation = String.class)),
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
            description = "Success",
            content = @Content(mediaType = "application/json")),
        @ApiResponse(responseCode = "404", description = "No session found for the given user and client id",
            content = @Content(schema = @Schema(implementation = String.class))),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        String userId = getHeader(HEADER_USER_ID, req, true);
        String clientId = getHeader(HEADER_CLIENT_ID, req, true);
        return sessionDictClient.get(GetRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setUserId(userId)
                .setClientId(clientId)
                .build())
            .thenApply(reply -> switch (reply.getResult()) {
                // return clientInfo as json response body
                case OK -> {
                    DefaultFullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                        Unpooled.wrappedBuffer(toJSON(reply.getOwner()).getBytes()));
                    resp.headers().set("Content-Type", "application/json");
                    yield resp;
                }
                case NOT_FOUND -> new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND, Unpooled.EMPTY_BUFFER);
                default -> new DefaultFullHttpResponse(req.protocolVersion(), INTERNAL_SERVER_ERROR,
                    Unpooled.EMPTY_BUFFER);
            });
    }

    @SneakyThrows
    private String toJSON(ClientInfo clientInfo) {
        return JsonFormat.printer().print(clientInfo);
    }
}
