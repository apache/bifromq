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

import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.apiserver.http.handler.utils.HeaderUtils;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.metaservice.IBaseKVStoreBalancerStatesProposal;

@Path("/store/balancer/rules")
final class GetLoadRulesHandler implements IHTTPRequestHandler {
    private final IBaseKVMetaService metaService;
    private final Map<String, IBaseKVStoreBalancerStatesProposal> balancerStateProposals = new ConcurrentHashMap<>();
    private final CompositeDisposable disposable = new CompositeDisposable();

    GetLoadRulesHandler(IBaseKVMetaService metaService) {
        this.metaService = metaService;
    }

    @Override
    public void start() {
        disposable.add(metaService.clusterIds().subscribe(clusterIds -> {
            balancerStateProposals.keySet().removeIf(clusterId -> !clusterIds.contains(clusterId));
            for (String clusterId : clusterIds) {
                balancerStateProposals.computeIfAbsent(clusterId, metaService::balancerStatesProposal);
            }
        }));
    }

    @Override
    public void close() {
        disposable.dispose();
        balancerStateProposals.values().forEach(IBaseKVStoreBalancerStatesProposal::stop);
    }

    @GET
    @Operation(summary = "Get the expected load rules of a store balancer")
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
            description = "Not load rules ever set for the store balancer",
            content = @Content(schema = @Schema(implementation = String.class))),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        try {
            String storeName = HeaderUtils.getHeader(Headers.HEADER_STORE_NAME, req, true);
            String balancerFactoryClass = HeaderUtils.getHeader(Headers.HEADER_BALANCER_FACTORY_CLASS, req, true);
            IBaseKVStoreBalancerStatesProposal statesProposal = balancerStateProposals.get(storeName);
            if (statesProposal == null) {
                return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                    Unpooled.copiedBuffer(("Store not found: " + storeName).getBytes())));
            }
            return statesProposal.expectedBalancerStates()
                .timeout(1, TimeUnit.SECONDS)
                .firstElement()
                .toCompletionStage()
                .toCompletableFuture()
                .handle((expectedStates, e) -> {
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
                        if (expectedStates.containsKey(balancerFactoryClass)) {
                            DefaultFullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                                Unpooled.wrappedBuffer(toJson(expectedStates.get(balancerFactoryClass).getLoadRules())
                                    .getBytes()));
                            resp.headers().set("Content-Type", "application/json");
                            return resp;
                        } else {
                            return new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND, Unpooled.EMPTY_BUFFER);
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
