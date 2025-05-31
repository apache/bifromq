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

package org.apache.bifromq.apiserver;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.apiserver.http.HTTPRequestRouter;
import org.apache.bifromq.apiserver.http.IHTTPRouteMap;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

@Slf4j
public class TLSServerInitializerTest extends MockableTest {
    @Mock
    private SslContext sslContext;
    @Mock
    private ByteBufAllocator byteBufAllocator;
    @Mock
    private IHTTPRouteMap routeMap;
    @Mock
    private ISettingProvider settingProvider;
    @Mock
    private SocketChannel mockChannel;
    @Mock
    private ChannelPipeline mockPipeline;

    @Test
    public void initChannel() {
        TLSServerInitializer serverInitializer = new TLSServerInitializer(sslContext, routeMap, settingProvider, 1024 * 1024);
        when(mockChannel.pipeline()).thenReturn(mockPipeline);
        when(mockChannel.alloc()).thenReturn(byteBufAllocator);

        serverInitializer.initChannel(mockChannel);

        ArgumentCaptor<ChannelHandler> handlersCaptor = ArgumentCaptor.forClass(ChannelHandler.class);
        verify(mockPipeline, times(5)).addLast(handlersCaptor.capture());
        verify(sslContext).newHandler(byteBufAllocator);
        List<ChannelHandler> handlers = handlersCaptor.getAllValues();
        assertTrue(handlers.get(1) instanceof HttpServerCodec);
        assertTrue(handlers.get(2) instanceof HttpObjectAggregator);
        assertTrue(handlers.get(3) instanceof HTTPRequestRouter);
        assertSame(handlers.get(4), ExceptionHandler.INSTANCE);
    }
}
