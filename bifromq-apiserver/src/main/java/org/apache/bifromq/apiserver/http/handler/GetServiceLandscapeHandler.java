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
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.apiserver.http.handler.utils.HeaderUtils;
import org.apache.bifromq.apiserver.http.handler.utils.JSONUtils;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceLandscape;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.baserpc.trafficgovernor.ServerEndpoint;

@Path("/service/landscape")
final class GetServiceLandscapeHandler extends AbstractTrafficRulesHandler implements IHTTPRequestHandler {
    GetServiceLandscapeHandler(IRPCServiceTrafficService trafficService) {
        super(trafficService);
    }

    @GET
    @Operation(summary = "Get the service landscape information")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id",
            schema = @Schema(implementation = Long.class)),
        @Parameter(name = "service_name", in = ParameterIn.HEADER, required = true,
            description = "the service name",
            schema = @Schema(implementation = String.class))
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
        IRPCServiceLandscape landscape = governorMap.get(serviceName);
        if (landscape == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Service not found: " + serviceName).getBytes())));
        }

        return landscape.serverEndpoints()
            .firstElement()
            .toCompletionStage()
            .toCompletableFuture()
            .thenApply(serverEndpoints -> {
                DefaultFullHttpResponse
                    resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(toJSON(serverEndpoints).getBytes()));
                resp.headers().set("Content-Type", "application/json");
                return resp;
            });
    }

    private String toJSON(Set<ServerEndpoint> landscape) {
        ObjectMapper mapper = JSONUtils.MAPPER;
        ArrayNode rootObject = mapper.createArrayNode();
        for (ServerEndpoint server : landscape) {
            ObjectNode serverObject = mapper.createObjectNode();
            serverObject.put("hostId", Base64.getEncoder().encodeToString(server.hostId().toByteArray()));
            serverObject.put("id", server.id());
            serverObject.put("address", server.address());
            serverObject.put("port", server.port());

            ObjectNode attrsObject = mapper.createObjectNode();
            for (String attrName : server.attrs().keySet()) {
                attrsObject.put(attrName, server.attrs().get(attrName));
            }
            serverObject.set("attributes", attrsObject);
            ArrayNode groupTagsArray = mapper.createArrayNode();
            for (String groupTag : server.groupTags()) {
                groupTagsArray.add(groupTag);
            }
            serverObject.set("groups", groupTagsArray);
            rootObject.add(serverObject);
        }
        return rootObject.toString();
    }
}
