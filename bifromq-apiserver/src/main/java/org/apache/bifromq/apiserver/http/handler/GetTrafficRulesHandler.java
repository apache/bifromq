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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.apiserver.http.handler.utils.HeaderUtils;
import org.apache.bifromq.apiserver.http.handler.utils.JSONUtils;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceLandscape;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;

@Path("/service/traffic")
final class GetTrafficRulesHandler extends AbstractTrafficRulesHandler implements IHTTPRequestHandler {
    GetTrafficRulesHandler(IRPCServiceTrafficService trafficService) {
        super(trafficService);
    }

    @GET
    @Operation(summary = "Get the traffic rules of a service")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "service_name", in = ParameterIn.HEADER, required = true,
            description = "the service name", schema = @Schema(implementation = String.class))
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
            description = "Success",
            content = @Content(mediaType = "application/json")),
        @ApiResponse(responseCode = "404",
            description = "Service not found",
            content = @Content(schema = @Schema(implementation = String.class))),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        String serviceName = HeaderUtils.getHeader(Headers.HEADER_SERVICE_NAME, req, true);
        if (serviceName == null || !isTrafficGovernable(serviceName)) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Service not found: " + serviceName).getBytes())));
        }
        IRPCServiceLandscape landscape = governorMap.get(serviceName);
        if (landscape == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Service not found: " + serviceName).getBytes())));
        }
        return landscape.trafficRules()
            .firstElement()
            .toCompletionStage()
            .toCompletableFuture()
            .thenApply(trafficDirective -> {
                DefaultFullHttpResponse
                    resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(toJSON(trafficDirective).getBytes()));
                resp.headers().set("Content-Type", "application/json");
                return resp;
            });
    }

    private String toJSON(Map<String, Map<String, Integer>> trafficDirective) {
        ObjectMapper mapper = JSONUtils.MAPPER;
        ObjectNode rootObject = mapper.createObjectNode();
        for (String tenantIdPrefix : trafficDirective.keySet()) {
            Map<String, Integer> groupWeights = trafficDirective.get(tenantIdPrefix);
            ObjectNode groupWeightsObject = mapper.createObjectNode();
            for (String group : groupWeights.keySet()) {
                groupWeightsObject.put(group, groupWeights.get(group));
            }
            rootObject.set(tenantIdPrefix, groupWeightsObject);
        }
        return rootObject.toString();
    }
}
