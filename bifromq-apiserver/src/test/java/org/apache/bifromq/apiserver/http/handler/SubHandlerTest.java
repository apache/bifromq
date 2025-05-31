/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_ID;
import static org.apache.bifromq.apiserver.Headers.HEADER_SUB_QOS;
import static org.apache.bifromq.apiserver.Headers.HEADER_TOPIC_FILTER;
import static org.apache.bifromq.apiserver.Headers.HEADER_USER_ID;
import static org.apache.bifromq.apiserver.http.handler.HeaderUtils.getRequiredSubQoS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.rpc.proto.SubReply;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SubHandlerTest extends AbstractHTTPRequestHandlerTest<SubHandler> {
    @Mock
    private ISessionDictClient sessionDictClient;

    @Override
    protected Class<SubHandler> handlerClass() {
        return SubHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        SubHandler handler = new SubHandler(settingProvider, sessionDictClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void sub() {
        sub(SubReply.Result.OK, HttpResponseStatus.OK);
        sub(SubReply.Result.EXISTS, HttpResponseStatus.OK);
        sub(SubReply.Result.NO_SESSION, HttpResponseStatus.NOT_FOUND);
        sub(SubReply.Result.NOT_AUTHORIZED, HttpResponseStatus.UNAUTHORIZED);
        sub(SubReply.Result.TOPIC_FILTER_INVALID, HttpResponseStatus.BAD_REQUEST);
        sub(SubReply.Result.EXCEED_LIMIT, HttpResponseStatus.BAD_REQUEST);
        sub(SubReply.Result.ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    private void sub(SubReply.Result subResult, HttpResponseStatus expectedStatus) {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "user");
        req.headers().set(HEADER_CLIENT_ID.header, "greeting_inbox");
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        req.headers().set(HEADER_SUB_QOS.header, "1");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        SubHandler handler = new SubHandler(settingProvider, sessionDictClient);
        when(sessionDictClient.sub(any()))
            .thenReturn(CompletableFuture.completedFuture(SubReply.newBuilder()
                .setResult(subResult)
                .build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        verify(sessionDictClient).sub(argThat(r -> r.getReqId() == reqId
            && r.getTenantId().equals(tenantId)
            && r.getTopicFilter().equals(req.headers().get(HEADER_TOPIC_FILTER.header))
            && r.getQos() == getRequiredSubQoS(req)
            && r.getClientId().equals(req.headers().get(HEADER_CLIENT_ID.header))));
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), expectedStatus);
        Mockito.reset(sessionDictClient);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.PUT);
    }
}
