/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/rules/load")
final class GetLoadRulesHandler extends AbstractLoadRulesHandler implements IHTTPRequestHandler {
    GetLoadRulesHandler(IBaseKVMetaService metaService) {
        super(metaService);
    }

    @GET
    @Operation(summary = "Get the load rules")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id",
            schema = @Schema(implementation = Long.class)),
        @Parameter(name = "store_name", in = ParameterIn.HEADER, required = true,
            description = "the service name",
            schema = @Schema(implementation = String.class)),
        @Parameter(name = "balancer_factory_class", in = ParameterIn.HEADER, required = true,
            description = "the full qualified name of balancer factory class configured for the store",
            schema = @Schema(implementation = String.class))
    })
    @RequestBody()
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
            description = "Success",
            content = @Content(mediaType = "application/json")),
        @ApiResponse(responseCode = "404",
            description = "Success",
            content = @Content(schema = @Schema(implementation = String.class))),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        try {
            log.trace("Handling http get load rules request: {}", req);
            String storeName = HeaderUtils.getHeader(Headers.HEADER_STORE_NAME, req, true);
            String balancerFactoryClass = HeaderUtils.getHeader(Headers.HEADER_BALANCER_FACTORY_CLASS, req, true);
            IBaseKVClusterMetadataManager metadataManager = metadataManagers.get(storeName);
            if (metadataManager == null) {
                return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                    Unpooled.copiedBuffer(("Store not found: " + storeName).getBytes())));
            }
            return metadataManager.loadRules()
                .timeout(1, TimeUnit.SECONDS)
                .firstElement()
                .toCompletionStage()
                .toCompletableFuture()
                .handle((loadRules, e) -> {
                    if (e != null) {
                        if (e instanceof TimeoutException) {
                            DefaultFullHttpResponse resp =
                                new DefaultFullHttpResponse(req.protocolVersion(), OK,
                                    Unpooled.wrappedBuffer(toJson(Struct.getDefaultInstance()).getBytes()));
                            resp.headers().set("Content-Type", "application/json");
                            return resp;
                        } else {
                            return new DefaultFullHttpResponse(req.protocolVersion(), INTERNAL_SERVER_ERROR,
                                Unpooled.copiedBuffer(e.getMessage().getBytes()));
                        }
                    } else {
                        if (loadRules.containsKey(balancerFactoryClass)) {
                            DefaultFullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                                Unpooled.wrappedBuffer(toJson(loadRules.get(balancerFactoryClass)).getBytes()));
                            resp.headers().set("Content-Type", "application/json");
                            return resp;
                        } else {
                            return new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                                Unpooled.copiedBuffer("NO_BALANCER".getBytes()));
                        }
                    }
                });
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @SneakyThrows
    private String toJson(Struct loadRules) {
        return JsonFormat.printer().print(loadRules);
    }
}
