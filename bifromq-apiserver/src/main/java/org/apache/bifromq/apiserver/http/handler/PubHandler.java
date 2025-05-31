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

import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static org.apache.bifromq.apiserver.Headers.HEADER_EXPIRY_SECONDS;
import static org.apache.bifromq.apiserver.Headers.HEADER_QOS;
import static org.apache.bifromq.apiserver.http.handler.HeaderUtils.getClientMeta;
import static org.apache.bifromq.apiserver.http.handler.HeaderUtils.getHeader;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.utils.TopicUtil;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import com.google.protobuf.ByteString;
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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/pub")
final class PubHandler extends TenantAwareHandler {
    private static final ByteBuf UNACCEPTED_TOPIC = Unpooled.wrappedBuffer("Unaccepted Topic".getBytes());
    private static final ByteBuf INVALID_QOS = Unpooled.wrappedBuffer("Invalid QoS".getBytes());
    private static final ByteBuf INVALID_EXPIRY_SECONDS = Unpooled.wrappedBuffer("Invalid expiry seconds".getBytes());
    private final IDistClient distClient;
    private final ISettingProvider settingProvider;

    PubHandler(ISettingProvider settingProvider, IDistClient distClient) {
        super(settingProvider);
        this.distClient = distClient;
        this.settingProvider = settingProvider;
    }

    @POST
    @Operation(summary = "Publish a message to given topic")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER, description = "optional caller provided request id",
            schema = @Schema(implementation = Long.class)),
        @Parameter(name = "tenant_id", in = ParameterIn.HEADER, required = true, description = "the tenant id",
            schema = @Schema(implementation = String.class)),
        @Parameter(name = "topic", in = ParameterIn.HEADER, required = true, description = "the message topic",
            schema = @Schema(implementation = String.class)),
        @Parameter(name = "qos", in = ParameterIn.HEADER, required = true,
            description = "QoS of the message to be published",
            schema = @Schema(implementation = Integer.class, allowableValues = {"0", "1", "2"})),
        @Parameter(name = "expiry_seconds", in = ParameterIn.HEADER,
            description = "the message expiry seconds, must be positive",
            schema = @Schema(implementation = Integer.class)),
        @Parameter(name = "client_type", in = ParameterIn.HEADER, required = true,
            description = "the caller client type", schema = @Schema(implementation = String.class)),
        @Parameter(name = "client_meta_*", in = ParameterIn.HEADER,
            description = "the metadata header about caller client, must be started with client_meta_"),
    })
    @RequestBody(required = true,
        description = "Message payload will be treated as binary",
        content = @Content(mediaType = "application/octet-stream"))
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success",
            content = @Content(schema = @Schema(implementation = String.class))),
        @ApiResponse(responseCode = "400", description = "Invalid QoS or expiry seconds",
            content = @Content(schema = @Schema(implementation = String.class))),
        @ApiResponse(responseCode = "403", description = "Unaccepted Topic",
            content = @Content(schema = @Schema(implementation = String.class))),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        String topic = getHeader(Headers.HEADER_TOPIC, req, true);
        String clientType = getHeader(HEADER_CLIENT_TYPE, req, true);
        int qos = Integer.parseInt(getHeader(HEADER_QOS, req, true));
        int expirySeconds = Optional.ofNullable(getHeader(HEADER_EXPIRY_SECONDS, req, false)).map(Integer::parseInt)
            .orElse(Integer.MAX_VALUE);
        Map<String, String> clientMeta = getClientMeta(req);
        log.trace("Handling http pub request: {}", req);
        if (!TopicUtil.checkTopicFilter(topic, tenantId, settingProvider)) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), FORBIDDEN, UNACCEPTED_TOPIC));
        }
        if (qos < 0 || qos > 2) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST, INVALID_QOS));
        }
        if (expirySeconds <= 0) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST, INVALID_EXPIRY_SECONDS));
        }
        ClientInfo clientInfo = ClientInfo.newBuilder()
            .setTenantId(tenantId)
            .setType(clientType)
            .putAllMetadata(clientMeta)
            .build();
        CompletableFuture<PubResult> distFuture = distClient.pub(reqId, topic,
            Message.newBuilder()
                .setMessageId(0)
                .setPubQoS(QoS.forNumber(qos))
                .setPayload(ByteString.copyFrom(req.content().nioBuffer()))
                .setExpiryInterval(expirySeconds)
                .setTimestamp(HLC.INST.get())
                .build(),
            clientInfo);
        return distFuture
            .thenApply(pubResult -> {
                switch (pubResult) {
                    case OK, NO_MATCH -> {
                        return new DefaultFullHttpResponse(req.protocolVersion(), OK,
                            Unpooled.wrappedBuffer(pubResult.name().getBytes()));
                    }
                    case BACK_PRESSURE_REJECTED -> {
                        return new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST,
                            Unpooled.wrappedBuffer(pubResult.name().getBytes()));
                    }
                    default -> {
                        return new DefaultFullHttpResponse(req.protocolVersion(), INTERNAL_SERVER_ERROR,
                            Unpooled.EMPTY_BUFFER);
                    }
                }
            });
    }
}
