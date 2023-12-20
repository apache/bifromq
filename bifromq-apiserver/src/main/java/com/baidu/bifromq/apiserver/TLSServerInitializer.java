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

package com.baidu.bifromq.apiserver;

import com.baidu.bifromq.apiserver.http.HTTPRequestRouter;
import com.baidu.bifromq.apiserver.http.IHTTPRouteMap;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;

public class TLSServerInitializer extends AbstractServerInitializer {
    private final SslContext sslContext;

    public TLSServerInitializer(SslContext sslContext, IHTTPRouteMap routeMap, ISettingProvider settingProvider) {
        super(routeMap, settingProvider);
        this.sslContext = sslContext;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
        ch.pipeline().addLast(new HttpServerCodec());
        ch.pipeline().addLast(new HTTPRequestRouter(routeMap, settingProvider));
        ch.pipeline().addLast(ExceptionHandler.INSTANCE);
    }
}
