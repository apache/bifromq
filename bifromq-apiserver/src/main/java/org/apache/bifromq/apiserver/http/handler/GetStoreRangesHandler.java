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
import static org.apache.bifromq.apiserver.http.handler.utils.JSONUtils.MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
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
import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.http.handler.utils.HeaderUtils;
import org.apache.bifromq.basekv.metaservice.IBaseKVLandscapeObserver;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;

@Path("/store/ranges")
class GetStoreRangesHandler extends AbstractLandscapeHandler {
    GetStoreRangesHandler(IBaseKVMetaService metaService) {
        super(metaService);
    }

    @GET
    @Operation(summary = "Get the ranges information in a store node")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER,
            description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "store_name", in = ParameterIn.HEADER, required = true,
            description = "the store name", schema = @Schema(implementation = String.class)),
        @Parameter(name = "store_id", in = ParameterIn.HEADER, required = true,
            description = "the store id", schema = @Schema(implementation = String.class))
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
        String storeName = HeaderUtils.getHeader(Headers.HEADER_STORE_NAME, req, true);
        IBaseKVLandscapeObserver landscapeObserver = landscapeObservers.get(storeName);
        if (landscapeObserver == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Store not found: " + storeName).getBytes())));
        }
        String storeId = HeaderUtils.getHeader(Headers.HEADER_STORE_ID, req, true);
        Optional<KVRangeStoreDescriptor> storeDescriptor = landscapeObserver.getStoreDescriptor(storeId);
        if (storeDescriptor.isEmpty()) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Store server not found: " + storeId).getBytes())));
        }

        DefaultFullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), OK,
            Unpooled.wrappedBuffer(toJSON(storeDescriptor.get().getRangesList()).getBytes()));
        resp.headers().set("Content-Type", "application/json");
        return CompletableFuture.completedFuture(resp);
    }

    private String toJSON(List<KVRangeDescriptor> rangeDescriptors) {
        ArrayNode rootObject = MAPPER.createArrayNode();
        for (KVRangeDescriptor rangeDescriptor : rangeDescriptors) {
            rootObject.add(toJSON(rangeDescriptor));
        }
        return rootObject.toString();
    }

    private JsonNode toJSON(KVRangeDescriptor descriptor) {
        ObjectNode rangeObject = MAPPER.createObjectNode();
        rangeObject.put("id", descriptor.getId().getEpoch() + "_" + descriptor.getId().getId());
        rangeObject.put("ver", descriptor.getVer());
        rangeObject.set("boundary", toJSON(descriptor.getBoundary()));
        rangeObject.put("state", descriptor.getState().name());
        rangeObject.put("role", descriptor.getRole().name());
        rangeObject.set("clusterConfig", toJSON(descriptor.getConfig()));
        return rangeObject;
    }

    private JsonNode toJSON(Boundary boundary) {
        ObjectNode boundaryObject = MAPPER.createObjectNode();
        boundaryObject.put("startKey", boundary.hasStartKey() ? toHex(boundary.getStartKey()) : null);
        boundaryObject.put("endKey", boundary.hasEndKey() ? toHex(boundary.getEndKey()) : null);
        return boundaryObject;
    }

    private JsonNode toJSON(ClusterConfig config) {
        ObjectNode clusterConfigObject = MAPPER.createObjectNode();

        ArrayNode votersArray = MAPPER.createArrayNode();
        config.getVotersList().forEach(votersArray::add);
        clusterConfigObject.set("voters", votersArray);

        ArrayNode learnersArray = MAPPER.createArrayNode();
        config.getLearnersList().forEach(learnersArray::add);
        clusterConfigObject.set("learners", learnersArray);

        ArrayNode nextVotersArray = MAPPER.createArrayNode();
        config.getNextVotersList().forEach(nextVotersArray::add);
        clusterConfigObject.set("nextVoters", nextVotersArray);

        ArrayNode nextLearnersArray = MAPPER.createArrayNode();
        config.getNextLearnersList().forEach(nextLearnersArray::add);
        clusterConfigObject.set("nextLearners", nextLearnersArray);

        return clusterConfigObject;
    }

    private String toHex(ByteString bs) {
        StringBuilder sb = new StringBuilder(bs.size() * 5);
        for (int i = 0; i < bs.size(); i++) {
            byte b = bs.byteAt(i);
            if (b >= 32 && b <= 126) {
                sb.append((char) b);
            } else {
                sb.append(String.format("0x%02X", b));
            }
        }
        return sb.toString();
    }
}
