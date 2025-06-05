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

import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_ID;
import static org.apache.bifromq.apiserver.Headers.HEADER_TOPIC_FILTER;
import static org.apache.bifromq.apiserver.Headers.HEADER_USER_ID;
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
import org.apache.bifromq.sessiondict.rpc.proto.UnsubReply;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UnsubHandlerTest extends AbstractHTTPRequestHandlerTest<UnsubHandler> {
    @Mock
    private ISessionDictClient sessionDictClient;

    @Override
    protected Class<UnsubHandler> handlerClass() {
        return UnsubHandler.class;
    }

    @Test
    public void missingHeaders() {
        DefaultFullHttpRequest req = buildRequest();
        UnsubHandler handler = new UnsubHandler(settingProvider, sessionDictClient);
        assertThrows(() -> handler.handle(123, "fakeTenant", req).join());
    }

    @Test
    public void unsub() {
        unsub(UnsubReply.Result.OK, HttpResponseStatus.OK);
        unsub(UnsubReply.Result.NO_SUB, HttpResponseStatus.OK);
        unsub(UnsubReply.Result.NO_SESSION, HttpResponseStatus.NOT_FOUND);
        unsub(UnsubReply.Result.NOT_AUTHORIZED, HttpResponseStatus.UNAUTHORIZED);
        unsub(UnsubReply.Result.TOPIC_FILTER_INVALID, HttpResponseStatus.BAD_REQUEST);
        unsub(UnsubReply.Result.ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    private void unsub(UnsubReply.Result result, HttpResponseStatus expectedStatus) {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "user");
        req.headers().set(HEADER_CLIENT_ID.header, "greeting_inbox");
        req.headers().set(HEADER_TOPIC_FILTER.header, "/greeting/#");
        long reqId = 123;
        String tenantId = "bifromq_dev";

        UnsubHandler handler = new UnsubHandler(settingProvider, sessionDictClient);
        when(sessionDictClient.unsub(any()))
            .thenReturn(CompletableFuture.completedFuture(UnsubReply.newBuilder()
                .setResult(result)
                .build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), expectedStatus);
        verify(sessionDictClient).unsub(argThat(r -> r.getReqId() == reqId
            && r.getTenantId().equals(tenantId)
            && r.getUserId().equals(req.headers().get(HEADER_USER_ID.header))
            && r.getTopicFilter().equals(req.headers().get(HEADER_TOPIC_FILTER.header))
            && r.getClientId().equals(req.headers().get(HEADER_CLIENT_ID.header))));
        Mockito.reset(sessionDictClient);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.DELETE);
    }
}
