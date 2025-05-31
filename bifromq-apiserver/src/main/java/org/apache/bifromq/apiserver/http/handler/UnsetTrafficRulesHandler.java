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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.util.CharsetUtil.UTF_8;

import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.swagger.v3.oas.annotations.Hidden;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/rules/traffic")
final class UnsetTrafficRulesHandler extends AbstractTrafficRulesHandler implements IHTTPRequestHandler {
    UnsetTrafficRulesHandler(IRPCServiceTrafficService trafficService) {
        super(trafficService);
    }

    @DELETE
    @Operation(summary = "Unset the traffic directive")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "service_name", in = ParameterIn.HEADER, required = true,
            description = "the service name", schema = @Schema(implementation = String.class)),
    })
    @RequestBody(content = @Content(mediaType = "application/json", schema = @Schema(implementation = String[].class)))
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
        @ApiResponse(responseCode = "400", description = "Bad Request"),
    })
    @Override
    @Hidden
    // This method is hidden now, it will be refactored in the future using json-patch
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        String serviceName = HeaderUtils.getHeader(Headers.HEADER_SERVICE_NAME, req, true);
        IRPCServiceTrafficGovernor governor = governorMap.get(serviceName);
        if (governor == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer((serviceName + " not found").getBytes())));
        }

        Set<String> trafficRules = new HashSet<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ArrayNode tdObject = (ArrayNode) objectMapper.readTree(req.content().toString(UTF_8));
            tdObject.forEach(node -> trafficRules.add(node.asText()));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.BAD_REQUEST,
                    Unpooled.copiedBuffer(e.getMessage().getBytes())));
        }
        log.trace("Handling http unset traffic directive request: {}", req);
        return CompletableFuture.allOf(trafficRules
                .stream()
                .map(governor::unsetTrafficRules)
                .toArray(CompletableFuture[]::new))
            .thenApply(
                trafficDirective -> new DefaultFullHttpResponse(req.protocolVersion(), OK, Unpooled.EMPTY_BUFFER));
    }
}
