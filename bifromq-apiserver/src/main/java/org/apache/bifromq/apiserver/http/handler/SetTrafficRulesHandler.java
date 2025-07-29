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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.apiserver.http.handler.utils.HeaderUtils;
import org.apache.bifromq.apiserver.http.handler.utils.JSONUtils;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;

@Path("/service/traffic")
final class SetTrafficRulesHandler extends AbstractTrafficRulesHandler implements IHTTPRequestHandler {
    SetTrafficRulesHandler(IRPCServiceTrafficService trafficService) {
        super(trafficService);
    }

    @PUT
    @Operation(summary = "Set the traffic rules of a service")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "service_name", in = ParameterIn.HEADER, required = true,
            description = "the service name", schema = @Schema(implementation = String.class)),
    })
    @RequestBody(content = @Content(mediaType = "application/json"))
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
        @ApiResponse(responseCode = "400", description = "Bad Request"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        String serviceName = HeaderUtils.getHeader(Headers.HEADER_SERVICE_NAME, req, true);
        if (!isTrafficGovernable(serviceName)) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Service not found: " + serviceName).getBytes())));
        }
        IRPCServiceTrafficGovernor governor = governorMap.get(serviceName);
        if (governor == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Service not found: " + serviceName).getBytes())));
        }

        Map<String, Map<String, Integer>> trafficRules = new HashMap<>();
        try {
            ObjectNode trObject = (ObjectNode) JSONUtils.MAPPER.readTree(req.content().toString(CharsetUtil.UTF_8));
            trObject.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                ObjectNode innerObject = (ObjectNode) entry.getValue();
                Map<String, Integer> innerMap = new HashMap<>();
                innerObject.fields()
                    .forEachRemaining(innerEntry -> innerMap.put(innerEntry.getKey(), innerEntry.getValue().asInt()));
                if (key.isEmpty()) {
                    throw new IllegalArgumentException("Empty tenantIdPrefix is not allowed");
                }
                trafficRules.put(key, innerMap);
            });
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.BAD_REQUEST,
                    Unpooled.copiedBuffer(e.getMessage().getBytes())));
        }
        return CompletableFuture.allOf(trafficRules.entrySet()
                .stream()
                .map(e -> governor.setTrafficRules(e.getKey(), e.getValue()))
                .toArray(CompletableFuture[]::new))
            .thenApply(
                trafficDirective -> new DefaultFullHttpResponse(req.protocolVersion(), OK, Unpooled.EMPTY_BUFFER));
    }
}
