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

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
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
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/cluster")
final class GetClusterHandler implements IHTTPRequestHandler {
    private final IAgentHost agentHost;

    GetClusterHandler(IAgentHost agentHost) {
        this.agentHost = agentHost;
    }

    @GET
    @Operation(summary = "Get cluster membership known from current node")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER, description = "optional caller provided request id",
            schema = @Schema(implementation = Long.class))})
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success",
            content = @Content(mediaType = "application/json")),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        log.trace("Handling http get cluster request: {}", req);
        return agentHost.membership().first(Set.of(agentHost.local()))
            .toCompletionStage()
            .toCompletableFuture()
            .thenApply(nodes -> {
                DefaultFullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(toJSON(nodes).getBytes()));
                resp.headers().set("Content-Type", "application/json");
                return resp;
            });
    }

    private String toJSON(Set<HostEndpoint> nodes) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootObject = mapper.createObjectNode();
        rootObject.put("env", agentHost.env());
        ArrayNode arrayNode = mapper.createArrayNode();
        rootObject.set("nodes", arrayNode);
        rootObject.put("local", Base64.getEncoder().encodeToString(agentHost.local().getId().toByteArray()));
        for (HostEndpoint node : nodes) {
            ObjectNode nodeObject = mapper.createObjectNode();
            nodeObject.put("id", Base64.getEncoder().encodeToString(node.getId().toByteArray()));
            nodeObject.put("address", node.getAddress());
            nodeObject.put("port", node.getPort());
            nodeObject.put("pid", node.getPid());
            arrayNode.add(nodeObject);
        }
        return rootObject.toString();
    }
}
