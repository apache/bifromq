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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.baserpc.trafficgovernor.ServerEndpoint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.rxjava3.core.Observable;
import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import org.testng.annotations.Test;

public class GetServiceLandscapeHandlerTest extends AbstractHTTPRequestHandlerTest<GetServiceLandscapeHandler> {
    @Override
    protected Class<GetServiceLandscapeHandler> handlerClass() {
        return GetServiceLandscapeHandler.class;
    }

    @SneakyThrows
    @Test
    public void testHandle() {
        ServerEndpoint serverEndpoint = new ServerEndpoint(ByteString.copyFromUtf8("agentHostId"), "server1",
            "host", 123,
            new InetSocketAddress(123),
            Set.of("group1", "group2"),
            Map.of("key1", "value1", "key2", "value2"), false);
        when(trafficService.services()).thenReturn(Observable.just(Set.of("test")));
        when(trafficGovernor.serverEndpoints()).thenReturn(Observable.just(Set.of(serverEndpoint)));

        GetServiceLandscapeHandler handler = new GetServiceLandscapeHandler(trafficService);
        handler.start();
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        req.headers().set("service_name", "test");
        FullHttpResponse resp = handler.handle(111, req).join();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.status(), HttpResponseStatus.OK);
        assertEquals(resp.headers().get("Content-Type"), "application/json");

        ObjectMapper objectMapper = new ObjectMapper();
        String responseContent = resp.content().toString(io.netty.util.CharsetUtil.UTF_8);
        ArrayNode jsonResponse = (ArrayNode) objectMapper.readTree(responseContent);

        assertEquals(jsonResponse.size(), 1);
        ObjectNode serverNode = (ObjectNode) jsonResponse.get(0);

        assertEquals(serverNode.get("hostId").asText(),
            Base64.getEncoder().encodeToString(serverEndpoint.hostId().toByteArray()));
        assertEquals(serverNode.get("address").asText(), serverEndpoint.address());
        assertEquals(serverNode.get("port").asInt(), serverEndpoint.port());

        ObjectNode attributes = (ObjectNode) serverNode.get("attributes");
        assertEquals(attributes.get("key1").asText(), "value1");
        assertEquals(attributes.get("key2").asText(), "value2");

        ArrayNode groups = (ArrayNode) serverNode.get("groups");
        assertEquals(groups.size(), 2);
        assertEquals(Set.of(groups.get(0).asText(), groups.get(1).asText()), Set.of("group1", "group2"));
    }
}
