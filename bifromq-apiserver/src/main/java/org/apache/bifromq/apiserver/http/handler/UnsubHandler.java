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

package org.apache.bifromq.apiserver.http.handler;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_ID;
import static org.apache.bifromq.apiserver.Headers.HEADER_TOPIC_FILTER;
import static org.apache.bifromq.apiserver.Headers.HEADER_USER_ID;
import static org.apache.bifromq.apiserver.http.handler.HeaderUtils.getHeader;

import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.rpc.proto.UnsubRequest;

@Slf4j
@Path("/unsub")
final class UnsubHandler extends TenantAwareHandler {
    private final ISessionDictClient sessionDictClient;

    UnsubHandler(ISettingProvider settingProvider, ISessionDictClient sessionDictClient) {
        super(settingProvider);
        this.sessionDictClient = sessionDictClient;
    }

    @DELETE
    @Operation(summary = "Remove a topic subscription from a mqtt session")
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
        @Parameter(name = "topic_filter", in = ParameterIn.HEADER, required = true,
            description = "the topic filter to remove", schema = @Schema(implementation = String.class)),
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
        @ApiResponse(responseCode = "401", description = "Unauthorized to remove subscription of the given topic filter"),
        @ApiResponse(responseCode = "404", description = "No session found for the given user and client id"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        String topicFilter = getHeader(HEADER_TOPIC_FILTER, req, true);
        String userId = getHeader(HEADER_USER_ID, req, true);
        String clientId = getHeader(HEADER_CLIENT_ID, req, true);
        log.trace("Handling http unsub request: {}", req);
        return sessionDictClient.unsub(UnsubRequest.newBuilder()
                .setReqId(reqId)
                .setTenantId(tenantId)
                .setUserId(userId)
                .setClientId(clientId)
                .setTopicFilter(topicFilter)
                .build())
            .thenApply(reply -> switch (reply.getResult()) {
                case OK, NO_SUB -> new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(reply.getResult().name().getBytes()));
                case TOPIC_FILTER_INVALID, BACK_PRESSURE_REJECTED ->
                    new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST,
                        Unpooled.wrappedBuffer(reply.getResult().name().getBytes()));
                case NO_SESSION -> new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND, Unpooled.EMPTY_BUFFER);
                case NOT_AUTHORIZED ->
                    new DefaultFullHttpResponse(req.protocolVersion(), UNAUTHORIZED, Unpooled.EMPTY_BUFFER);
                default -> new DefaultFullHttpResponse(req.protocolVersion(), INTERNAL_SERVER_ERROR,
                    Unpooled.EMPTY_BUFFER);
            });
    }
}
