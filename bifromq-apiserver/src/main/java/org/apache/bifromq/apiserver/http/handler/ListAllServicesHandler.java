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

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/landscape/services")
final class ListAllServicesHandler extends AbstractTrafficRulesHandler {

    ListAllServicesHandler(IRPCServiceTrafficService trafficService) {
        super(trafficService);
    }


    @GET
    @Operation(summary = "List the name of sub-services in the cluster")
    @Parameters({
        @Parameter(name = "req_id",
            in = ParameterIn.HEADER,
            description = "optional caller provided request id",
            schema = @Schema(implementation = Long.class)),
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
            description = "Success",
            content = @Content(mediaType = "application/json"))
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        log.trace("Handling http get service landscape request: {}", req);
        Set<String> serviceUniqueNames = governorMap.keySet();
        DefaultFullHttpResponse
            resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
            Unpooled.wrappedBuffer(toJSON(serviceUniqueNames).getBytes()));
        resp.headers().set("Content-Type", "application/json");
        return CompletableFuture.completedFuture(resp);
    }

    private String toJSON(Set<String> serviceUniqueNames) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode rootObject = mapper.createArrayNode();
        for (String name : serviceUniqueNames) {
            rootObject.add(name);
        }
        return rootObject.toString();
    }
}
