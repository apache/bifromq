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

import static org.apache.bifromq.apiserver.Headers.HEADER_TENANT_ID;
import static org.apache.bifromq.apiserver.http.handler.HeaderUtils.getHeader;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import com.google.common.base.Strings;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import java.util.concurrent.CompletableFuture;

abstract class TenantAwareHandler implements IHTTPRequestHandler {
    private final ISettingProvider settingProvider;

    protected TenantAwareHandler(ISettingProvider settingProvider) {
        this.settingProvider = settingProvider;
    }

    @Override
    public final CompletableFuture<FullHttpResponse> handle(long reqId, FullHttpRequest req) {
        String tenantId = getHeader(HEADER_TENANT_ID, req, false);
        if (Strings.isNullOrEmpty(tenantId)) {
            FullHttpResponse response =
                new DefaultFullHttpResponse(req.protocolVersion(), HttpResponseStatus.BAD_REQUEST, EMPTY_BUFFER);
            response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
            response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
            return CompletableFuture.completedFuture(response);
        }
        Integer maxUserPayloadBytes = settingProvider.provide(Setting.MaxUserPayloadBytes, tenantId);
        if (HttpUtil.getContentLength(req, -1) > maxUserPayloadBytes) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                req.protocolVersion(), HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, Unpooled.EMPTY_BUFFER);
            response.headers().set(CONTENT_LENGTH, 0);
            return CompletableFuture.completedFuture(response);
        }
        req.retain();
        return handle(reqId, tenantId, req);
    }

    protected abstract CompletableFuture<FullHttpResponse> handle(long reqId, String tenantId, FullHttpRequest req);
}
