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

import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_META_PREFIX;
import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_TYPE;
import static org.apache.bifromq.apiserver.Headers.HEADER_QOS;
import static org.apache.bifromq.apiserver.Headers.HEADER_RETAIN;
import static org.apache.bifromq.apiserver.Headers.HEADER_TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class RetainHandlerTest extends AbstractHTTPRequestHandlerTest<RetainHandler> {
    @Mock
    private IRetainClient retainClient;
    private ISettingProvider settingProvider = Setting::current;

    @Override
    protected Class<RetainHandler> handlerClass() {
        return RetainHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();

        RetainHandler handler = new RetainHandler(settingProvider, retainClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void retainResultHandling() {
        retain(RetainReply.Result.RETAINED, HttpResponseStatus.OK);
        retain(RetainReply.Result.CLEARED, HttpResponseStatus.OK);
        retain(RetainReply.Result.ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    private void retain(RetainReply.Result result, HttpResponseStatus expectedStatus) {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_TOPIC.header, "/greeting");
        req.headers().set(HEADER_CLIENT_TYPE.header, "admin_team");
        req.headers().set(HEADER_QOS.header, "2");
        req.headers().set(HEADER_RETAIN.header, "true");
        req.headers().set(HEADER_CLIENT_META_PREFIX + "age", "4");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        RetainHandler handler = new RetainHandler(settingProvider, retainClient);

        when(retainClient.retain(anyLong(), anyString(), any(), any(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(RetainReply.newBuilder()
                .setResult(result).build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), expectedStatus);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.POST);
    }
}
