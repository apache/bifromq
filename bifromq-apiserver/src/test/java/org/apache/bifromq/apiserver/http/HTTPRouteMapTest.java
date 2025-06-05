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

package org.apache.bifromq.apiserver.http;

import static org.testng.Assert.assertEquals;

import org.apache.bifromq.apiserver.MockableTest;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.retain.client.IRetainClient;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Collections;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class HTTPRouteMapTest extends MockableTest {
    @Mock
    private IDistClient distClient;
    @Mock
    private IRetainClient retainClient;
    private ISettingProvider settingProvider = Setting::current;

    @Test
    public void fallbackHandler() {
        HTTPRouteMap routeMap = new HTTPRouteMap(Collections.emptyList());
        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/fake");
        IHTTPRequestHandler handler =
            routeMap.getHandler(httpRequest);
        FullHttpResponse response = handler.handle(123, httpRequest).join();
        assertEquals(response.protocolVersion(), httpRequest.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.BAD_REQUEST);
        assertEquals(response.content().readableBytes(), 0);
    }
}
