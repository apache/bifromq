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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.rxjava3.core.Observable;
import java.util.Set;
import lombok.SneakyThrows;
import org.testng.annotations.Test;

public class ListAllServicesHandlerTest extends AbstractHTTPRequestHandlerTest<ListAllServicesHandler> {
    @Override
    protected Class<ListAllServicesHandler> handlerClass() {
        return ListAllServicesHandler.class;
    }

    @SneakyThrows
    @Test
    public void testHandle() {
        when(trafficService.services()).thenReturn(Observable.just(Set.of("testService")));

        ListAllServicesHandler handler = new ListAllServicesHandler(trafficService);
        handler.start();
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        FullHttpResponse resp = handler.handle(111, req).join();
        assertEquals(resp.protocolVersion(), req.protocolVersion());
        assertEquals(resp.status(), HttpResponseStatus.OK);
        assertEquals(resp.headers().get("Content-Type"), "application/json");

        ObjectMapper objectMapper = new ObjectMapper();
        String responseContent = resp.content().toString(io.netty.util.CharsetUtil.UTF_8);
        ArrayNode jsonResponse = (ArrayNode) objectMapper.readTree(responseContent);

        assertEquals(jsonResponse.size(), 1);
        assertEquals(jsonResponse.get(0).asText(), "testService");

    }
}
