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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.baidu.bifromq.apiserver.http.HTTPRequestRouter;
import com.baidu.bifromq.apiserver.http.IHTTPRouteMap;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

@Slf4j
public class NonTLSServerInitializerTest extends MockableTest {
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
        NonTLSServerInitializer serverInitializer = new NonTLSServerInitializer(routeMap, settingProvider);
        when(mockChannel.pipeline()).thenReturn(mockPipeline);

        serverInitializer.initChannel(mockChannel);

        ArgumentCaptor<ChannelHandler> handlersCaptor = ArgumentCaptor.forClass(ChannelHandler.class);
        verify(mockPipeline, times(3)).addLast(handlersCaptor.capture());
        List<ChannelHandler> handlers = handlersCaptor.getAllValues();
        assertTrue(handlers.get(0) instanceof HttpServerCodec);
        assertTrue(handlers.get(1) instanceof HTTPRequestRouter);
        assertSame(handlers.get(2), ExceptionHandler.INSTANCE);
    }
}
