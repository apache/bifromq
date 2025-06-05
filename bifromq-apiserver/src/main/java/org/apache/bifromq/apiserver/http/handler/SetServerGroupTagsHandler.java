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

import static org.apache.bifromq.apiserver.Headers.HEADER_SERVER_ID;
import static org.apache.bifromq.apiserver.http.handler.HeaderUtils.getHeader;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/rules/group")
final class SetServerGroupTagsHandler extends AbstractTrafficRulesHandler implements IHTTPRequestHandler {

    SetServerGroupTagsHandler(IRPCServiceTrafficService trafficService) {
        super(trafficService);
    }

    @PUT
    @Operation(summary = "Set server groups")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "service_name", in = ParameterIn.HEADER, required = true,
            description = "the service name", schema = @Schema(implementation = String.class)),
        @Parameter(name = "server_id", in = ParameterIn.HEADER, required = true,
            description = "the service server id", schema = @Schema(implementation = String.class)),
    })
    @RequestBody(required = true, content = @Content(
        mediaType = "application/json",
        schema = @Schema(implementation = String[].class)))
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
        @ApiResponse(responseCode = "400", description = "Bad Request"),
        @ApiResponse(responseCode = "404", description = "Service or server not found"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        String serverId = getHeader(HEADER_SERVER_ID, req, true);
        String serviceName = HeaderUtils.getHeader(Headers.HEADER_SERVICE_NAME, req, true);
        IRPCServiceTrafficGovernor governor = governorMap.get(serviceName);
        if (governor == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Service not found: " + serviceName).getBytes())));
        }

        Set<String> groupTags = new HashSet<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ArrayNode groupTagsArray =
                (ArrayNode) objectMapper.readTree(req.content().toString(io.netty.util.CharsetUtil.UTF_8));
            groupTagsArray.forEach(tagNode -> groupTags.add(tagNode.asText()));
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.BAD_REQUEST,
                    Unpooled.EMPTY_BUFFER));
        }
        log.trace("Handling http set server group tags request: {}", req);
        return governor.setServerGroups(serverId, groupTags)
            .handle((v, e) -> {
                if (e != null) {
                    return new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                        Unpooled.wrappedBuffer(e.getMessage().getBytes()));
                } else {
                    return new DefaultFullHttpResponse(req.protocolVersion(), OK, Unpooled.EMPTY_BUFFER);
                }
            });
    }
}
