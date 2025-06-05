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

import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.basekv.metaservice.IBaseKVClusterMetadataManager;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/landscape/store/ranges")
class GetStoreRangesHandler extends AbstractLoadRulesHandler {
    GetStoreRangesHandler(IBaseKVMetaService metaService) {
        super(metaService);
    }

    @GET
    @Operation(summary = "Get the store ranges information")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "store_name", in = ParameterIn.HEADER, required = true,
            description = "the store name", schema = @Schema(implementation = String.class)),
        @Parameter(name = "server_id", in = ParameterIn.HEADER, required = true,
            description = "the store server id", schema = @Schema(implementation = String.class))
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200",
            description = "Success",
            content = @Content(mediaType = "application/json")),
        @ApiResponse(responseCode = "404",
            description = "Store or store server not found",
            content = @Content(schema = @Schema(implementation = String.class))),

    })
    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        log.trace("Handling http get store ranges request: {}", req);
        String storeName = HeaderUtils.getHeader(Headers.HEADER_STORE_NAME, req, true);
        IBaseKVClusterMetadataManager metadataManager = metadataManagers.get(storeName);
        if (metadataManager == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Store not found: " + storeName).getBytes())));
        }
        String serverId = HeaderUtils.getHeader(Headers.HEADER_SERVER_ID, req, true);
        Optional<KVRangeStoreDescriptor> storeDescriptor = metadataManager.getStoreDescriptor(serverId);
        if (storeDescriptor.isEmpty()) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Server not found: " + serverId).getBytes())));
        }

        DefaultFullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
            Unpooled.wrappedBuffer(toJSON(storeDescriptor.get().getRangesList()).getBytes()));
        resp.headers().set("Content-Type", "application/json");
        return CompletableFuture.completedFuture(resp);
    }

    private String toJSON(List<KVRangeDescriptor> rangeDescriptors) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode rootObject = mapper.createArrayNode();
        for (KVRangeDescriptor rangeDescriptor : rangeDescriptors) {
            rootObject.add(JSONUtils.toJSON(rangeDescriptor, mapper));
        }
        return rootObject.toString();
    }
}
