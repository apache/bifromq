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

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;
import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.apiserver.http.IHTTPRequestHandler;
import org.apache.bifromq.apiserver.http.handler.utils.HeaderUtils;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.metaservice.IBaseKVStoreBalancerStatesProposer;

abstract class SetBalancerStateHandler extends AbstractBalancerStateProposerHandler implements IHTTPRequestHandler {
    SetBalancerStateHandler(IBaseKVMetaService metaService) {
        super(metaService);
    }

    protected CompletableFuture<FullHttpResponse> handle(long reqId, FullHttpRequest req, boolean disable) {
        String storeName = HeaderUtils.getHeader(Headers.HEADER_STORE_NAME, req, true);
        String balancerFactoryClass = HeaderUtils.getHeader(Headers.HEADER_BALANCER_FACTORY_CLASS, req, true);
        IBaseKVStoreBalancerStatesProposer statesProposer = balancerStateProposers.get(storeName);
        if (statesProposer == null) {
            return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                Unpooled.copiedBuffer(("Store not found: " + storeName).getBytes())));
        }
        try {
            return statesProposer.proposeRunState(balancerFactoryClass, disable)
                .handle(unwrap((v, e) -> {
                    if (e != null) {
                        if (e instanceof TimeoutException) {
                            return new DefaultFullHttpResponse(req.protocolVersion(),
                                REQUEST_TIMEOUT, EMPTY_BUFFER);
                        } else {
                            return new DefaultFullHttpResponse(req.protocolVersion(),
                                INTERNAL_SERVER_ERROR, Unpooled.copiedBuffer(e.getMessage().getBytes()));
                        }
                    }
                    switch (v) {
                        case ACCEPTED -> {
                            return new DefaultFullHttpResponse(req.protocolVersion(), OK, EMPTY_BUFFER);
                        }
                        default -> {
                            return new DefaultFullHttpResponse(req.protocolVersion(), CONFLICT, EMPTY_BUFFER);
                        }
                    }
                }));
        } catch (Throwable e) {
            return CompletableFuture.completedFuture(
                new DefaultFullHttpResponse(req.protocolVersion(), INTERNAL_SERVER_ERROR,
                    Unpooled.copiedBuffer(e.getMessage().getBytes())));
        }
    }
}
