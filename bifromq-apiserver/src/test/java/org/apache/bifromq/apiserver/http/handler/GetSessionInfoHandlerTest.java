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
import static org.apache.bifromq.apiserver.Headers.HEADER_USER_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.UnsafeByteOperations;
import com.google.protobuf.util.JsonFormat;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.apache.bifromq.sessiondict.rpc.proto.GetReply;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MQTTClientInfoConstants;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class GetSessionInfoHandlerTest extends AbstractHTTPRequestHandlerTest<GetSessionInfoHandler> {
    @Mock
    private ISessionDictClient sessionDictClient;

    @Override
    protected Class<GetSessionInfoHandler> handlerClass() {
        return GetSessionInfoHandler.class;
    }

    @Test
    public void noSession() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "user");
        req.headers().set(HEADER_CLIENT_ID.header, "client_id");
        long reqId = 123;
        String tenantId = "bifromq_dev";
        GetSessionInfoHandler handler = new GetSessionInfoHandler(settingProvider, sessionDictClient);
        when(sessionDictClient.get(any()))
            .thenReturn(CompletableFuture.completedFuture(GetReply.newBuilder()
                .setReqId(reqId)
                .setResult(GetReply.Result.NOT_FOUND)
                .build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        verify(sessionDictClient).get(argThat(r -> r.getReqId() == reqId
            && r.getTenantId().equals(tenantId)
            && r.getUserId().equals(req.headers().get(HEADER_USER_ID.header))
            && r.getClientId().equals(req.headers().get(HEADER_CLIENT_ID.header))));
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.NOT_FOUND);
    }

    @SneakyThrows
    @Test
    public void found() {
        DefaultFullHttpRequest req = buildRequest();
        req.headers().set(HEADER_USER_ID.header, "user");
        req.headers().set(HEADER_CLIENT_ID.header, "client_id");
        long reqId = 123;
        String tenantId = "bifromq_dev";
        ClientInfo owner = ClientInfo.newBuilder()
            .setTenantId(tenantId)
            .setType(MQTTClientInfoConstants.MQTT_TYPE_VALUE)
            .putMetadata(MQTTClientInfoConstants.MQTT_USER_ID_KEY, "user")
            .build();
        GetSessionInfoHandler handler = new GetSessionInfoHandler(settingProvider, sessionDictClient);
        when(sessionDictClient.get(any()))
            .thenReturn(CompletableFuture.completedFuture(GetReply.newBuilder()
                .setReqId(reqId)
                .setResult(GetReply.Result.OK)
                .setOwner(owner)
                .build()));
        FullHttpResponse response = handler.handle(reqId, tenantId, req).join();
        verify(sessionDictClient).get(argThat(r -> r.getReqId() == reqId
            && r.getTenantId().equals(tenantId)
            && r.getUserId().equals(req.headers().get(HEADER_USER_ID.header))
            && r.getClientId().equals(req.headers().get(HEADER_CLIENT_ID.header))));
        assertEquals(response.protocolVersion(), req.protocolVersion());
        assertEquals(response.status(), HttpResponseStatus.OK);
        assertEquals(response.headers().get("Content-Type"), "application/json");
        ClientInfo.Builder clientInfoBuilder = ClientInfo.newBuilder();
        String json = UnsafeByteOperations.unsafeWrap(response.content().duplicate().nioBuffer()).toStringUtf8();
        JsonFormat.parser().merge(json, clientInfoBuilder);
        assertEquals(clientInfoBuilder.build(), owner);
    }

    private DefaultFullHttpRequest buildRequest() {
        return buildRequest(HttpMethod.GET);
    }
}
