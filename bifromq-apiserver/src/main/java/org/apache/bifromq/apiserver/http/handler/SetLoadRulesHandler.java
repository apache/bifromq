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

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;

import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.basekv.metaservice.IBaseKVClusterMetadataManager;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import com.google.protobuf.Struct;
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
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/rules/load")
final class SetLoadRulesHandler extends AbstractLoadRulesHandler implements IHTTPRequestHandler {
    SetLoadRulesHandler(IBaseKVMetaService metaService) {
        super(metaService);
    }

    @PUT
    @Operation(summary = "Set the load rules")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "store_name", in = ParameterIn.HEADER, required = true,
            description = "the store name", schema = @Schema(implementation = String.class)),
        @Parameter(name = "balancer_factory_class", in = ParameterIn.HEADER, required = true,
            description = "the full qualified name of balancer factory class configured for the store",
            schema = @Schema(implementation = String.class))
    })
    @RequestBody(content = @Content(mediaType = "application/json"))
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        log.trace("Handling http set load rules request: {}", req);
        String storeName = HeaderUtils.getHeader(Headers.HEADER_STORE_NAME, req, true);
        String balancerFactoryClass = HeaderUtils.getHeader(Headers.HEADER_BALANCER_FACTORY_CLASS, req, true);
        IBaseKVClusterMetadataManager metadataManager = metadataManagers.get(storeName);
        if (metadataManager == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Store not found: " + storeName).getBytes())));
        }
        try {
            Struct loadRules = fromJson(req.content().toString(io.netty.util.CharsetUtil.UTF_8));
            return metadataManager.proposeLoadRules(balancerFactoryClass, loadRules)
                .handle(unwrap((v, e) -> {
                    if (e != null) {
                        if (e instanceof TimeoutException) {
                            return new DefaultFullHttpResponse(req.protocolVersion(),
                                REQUEST_TIMEOUT, EMPTY_BUFFER);
                        } else {
                            return new DefaultFullHttpResponse(req.protocolVersion(),
                                INTERNAL_SERVER_ERROR, Unpooled.copiedBuffer(e.getMessage().getBytes()));
                        }
                    }
                    switch (v) {
                        case ACCEPTED -> {
                            return new DefaultFullHttpResponse(req.protocolVersion(), OK, EMPTY_BUFFER);
                        }
                        case REJECTED -> {
                            return new DefaultFullHttpResponse(req.protocolVersion(), BAD_REQUEST, EMPTY_BUFFER);
                        }
                        case NO_BALANCER -> {
                            return new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                                Unpooled.copiedBuffer(v.name().getBytes()));
                        }
                        default -> {
                            return new DefaultFullHttpResponse(req.protocolVersion(), CONFLICT, EMPTY_BUFFER);
                        }
                    }
                }));
        } catch (Throwable e) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), INTERNAL_SERVER_ERROR,
                    Unpooled.copiedBuffer(e.getMessage().getBytes())));
        }
    }

    private Struct fromJson(String json) throws IOException {
        Struct.Builder structBuilder = Struct.newBuilder();
        JsonFormat.parser().ignoringUnknownFields().merge(json, structBuilder);
        return structBuilder.build();
    }
}
