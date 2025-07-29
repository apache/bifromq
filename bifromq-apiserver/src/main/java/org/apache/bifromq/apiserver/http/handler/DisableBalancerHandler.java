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
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;

@Path("/store/balancer/disable")
final class DisableBalancerHandler extends SetBalancerStateHandler implements IHTTPRequestHandler {
    DisableBalancerHandler(IBaseKVMetaService metaService) {
        super(metaService);
    }

    @PUT
    @Operation(summary = "Disable store balancer")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "store_name", in = ParameterIn.HEADER, required = true,
            description = "the store name", schema = @Schema(implementation = String.class)),
        @Parameter(name = "balancer_factory_class", in = ParameterIn.HEADER, required = true,
            description = "the full qualified name of balancer factory class configured for the store",
            schema = @Schema(implementation = String.class)),
    })
    @RequestBody(content = @Content(mediaType = "application/json"))
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        return handle(reqId, req, true);
    }
}
