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

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_ID;
import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static org.apache.bifromq.apiserver.Headers.HEADER_SERVER_REDIRECT;
import static org.apache.bifromq.apiserver.Headers.HEADER_SERVER_REFERENCE;
import static org.apache.bifromq.apiserver.Headers.HEADER_USER_ID;
import static org.apache.bifromq.apiserver.http.handler.HeaderUtils.getClientMeta;
import static org.apache.bifromq.apiserver.http.handler.HeaderUtils.getHeader;

import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import com.google.common.base.Strings;
import io.netty.buffer.ByteBuf;
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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.rpc.proto.KillAllReply;
import org.apache.bifromq.sessiondict.rpc.proto.KillReply;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.type.ClientInfo;

@Slf4j
@Path("/kill")
final class KillHandler extends TenantAwareHandler {
    private static final int MAX_SERVER_REFERENCE_LENGTH = 65535;
    private static final String SERVER_REDIRECT_VALUE_NO = "no";
    private static final String SERVER_REDIRECT_VALUE_MOVE = "move";
    private static final String SERVER_REDIRECT_VALUE_TEMP_USE = "temp_use";
    private static final Set<String> SERVER_REDIRECT_VALUES =
        Set.of(SERVER_REDIRECT_VALUE_NO, SERVER_REDIRECT_VALUE_MOVE, SERVER_REDIRECT_VALUE_TEMP_USE);
    private static final ByteBuf INVALID_SERVER_REDIRECT =
        Unpooled.wrappedBuffer("Invalid server redirect value".getBytes());
    private static final ByteBuf TOO_LONG_SERVER_REFERENCE =
        Unpooled.wrappedBuffer("Server reference exceeds 65535 bytes".getBytes());
    private final ISessionDictClient sessionDictClient;

    KillHandler(ISettingProvider settingProvider, ISessionDictClient sessionDictClient) {
        super(settingProvider);
        this.sessionDictClient = sessionDictClient;
    }

    @DELETE
    @Operation(summary = "Disconnect a MQTT client connection")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER, description = "optional caller provided request id",
            schema = @Schema(implementation = Long.class)),
        @Parameter(name = "tenant_id", in = ParameterIn.HEADER, required = true, description = "the tenant id",
            schema = @Schema(implementation = String.class)),
        @Parameter(name = "user_id", in = ParameterIn.HEADER,
            description = "the user id of the MQTT client connection to be disconnected",
            schema = @Schema(implementation = String.class)),
        @Parameter(name = "client_id", in = ParameterIn.HEADER,
            description = "the client id of the mqtt session"),
        @Parameter(name = "server_redirect", in = ParameterIn.HEADER,
            description = "indicate if the client should redirect to another server",
            schema = @Schema(implementation = String.class,
                allowableValues = {SERVER_REDIRECT_VALUE_NO, SERVER_REDIRECT_VALUE_MOVE,
                    SERVER_REDIRECT_VALUE_TEMP_USE})),
        @Parameter(name = "server_reference", in = ParameterIn.HEADER,
            description = "indicate the server reference to redirect to",
            schema = @Schema(implementation = String.class, maxLength = MAX_SERVER_REFERENCE_LENGTH)),
        @Parameter(name = "client_type", in = ParameterIn.HEADER, required = true,
            description = "the caller client type", schema = @Schema(implementation = String.class)),
        @Parameter(name = "client_meta_*", in = ParameterIn.HEADER,
            description = "the metadata header about the caller client, must be started with client_meta_"),
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
        @ApiResponse(responseCode = "404", description = "Not Found",
            content = @Content(schema = @Schema(implementation = String.class))),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        String userId = getHeader(HEADER_USER_ID, req, false);
        String clientId = getHeader(HEADER_CLIENT_ID, req, false);
        String serverRedirect = getHeader(HEADER_SERVER_REDIRECT, req, false);
        String serverReference = getHeader(HEADER_SERVER_REFERENCE, req, false);
        String clientType = getHeader(HEADER_CLIENT_TYPE, req, true);
        Map<String, String> clientMeta = getClientMeta(req);
        if (serverRedirect != null && !SERVER_REDIRECT_VALUES.contains(serverRedirect)) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST, INVALID_SERVER_REDIRECT));
        }
        if (serverReference != null && serverReference.length() > MAX_SERVER_REFERENCE_LENGTH) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST, TOO_LONG_SERVER_REFERENCE));
        }

        ServerRedirection serverRedirection = buildServerRedirection(serverRedirect, serverReference);
        log.trace("Handling http kill request: {}", req);
        if (Strings.isNullOrEmpty(userId) || Strings.isNullOrEmpty(clientId)) {
            return sessionDictClient.killAll(reqId, tenantId, userId, ClientInfo.newBuilder()
                    .setTenantId(tenantId)
                    .setType(clientType)
                    .putAllMetadata(clientMeta)
                    .build(), serverRedirection)
                .thenApply(
                    v -> new DefaultFullHttpResponse(req.protocolVersion(), v.getResult() == KillAllReply.Result.OK
                        ? OK : NOT_FOUND, Unpooled.EMPTY_BUFFER));
        }
        return sessionDictClient.kill(reqId, tenantId, userId, clientId, ClientInfo.newBuilder()
                .setTenantId(tenantId)
                .setType(clientType)
                .putAllMetadata(clientMeta)
                .build(), serverRedirection)
            .thenApply(v -> new DefaultFullHttpResponse(req.protocolVersion(), v.getResult() == KillReply.Result.OK
                ? OK : NOT_FOUND, Unpooled.EMPTY_BUFFER));
    }

    private ServerRedirection buildServerRedirection(String serverRedirect, String serverReference) {
        if (Strings.isNullOrEmpty(serverRedirect)) {
            return ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build();
        }
        switch (serverRedirect) {
            case SERVER_REDIRECT_VALUE_MOVE: {
                ServerRedirection.Builder builder = ServerRedirection.newBuilder()
                    .setType(ServerRedirection.Type.PERMANENT_MOVE);
                if (!Strings.isNullOrEmpty(serverReference)) {
                    builder.setServerReference(serverReference);
                }
                return builder.build();
            }
            case SERVER_REDIRECT_VALUE_TEMP_USE: {
                ServerRedirection.Builder builder = ServerRedirection.newBuilder()
                    .setType(ServerRedirection.Type.TEMPORARY_MOVE);
                if (!Strings.isNullOrEmpty(serverReference)) {
                    builder.setServerReference(serverReference);
                }
                return builder.build();
            }
            case SERVER_REDIRECT_VALUE_NO:
            default:
                return ServerRedirection.newBuilder().setType(ServerRedirection.Type.NO_MOVE).build();
        }
    }
}
