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

package org.apache.bifromq.apiserver.http;

import static org.apache.bifromq.apiserver.http.AnnotationUtil.getHTTPMethod;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import jakarta.ws.rs.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HTTPRouteMap implements IHTTPRouteMap {
    private static final IHTTPRequestHandler NO_ROUTE_HANDLER = (reqId, httpRequest) ->
        CompletableFuture.completedFuture(
            new DefaultFullHttpResponse(httpRequest.protocolVersion(), BAD_REQUEST, Unpooled.EMPTY_BUFFER));
    private final Map<String, Map<HttpMethod, IHTTPRequestHandler>> routeMap = new HashMap<>();


    public HTTPRouteMap(Collection<? extends IHTTPRequestHandler> handlers) {
        for (IHTTPRequestHandler handler : handlers) {
            Path route = handler.getClass().getAnnotation(Path.class);
            if (route != null) {
                Map<HttpMethod, IHTTPRequestHandler> methodsMap =
                    routeMap.computeIfAbsent(route.value(), k -> new HashMap<>());
                HttpMethod method = getHTTPMethod(handler.getClass());
                if (method != null) {
                    IHTTPRequestHandler prev = methodsMap.put(method, handler);
                    assert prev == null : "Path conflict: " + route.value();
                } else {
                    log.warn("No http method specified for http request handler: {}", handler.getClass().getName());
                }
            } else {
                log.warn("No Route annotation found for HTTPRequestHandler: {}", handler.getClass().getName());
            }
        }
    }

    public IHTTPRequestHandler getHandler(HttpRequest req) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(req.uri());
        return routeMap.getOrDefault(queryStringDecoder.path(), Collections.emptyMap())
            .getOrDefault(req.method(), NO_ROUTE_HANDLER);
    }
}
