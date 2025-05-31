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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import org.apache.bifromq.apiserver.MockableTest;
import org.apache.bifromq.basekv.metaservice.IBaseKVClusterMetadataManager;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import jakarta.ws.rs.Path;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public abstract class AbstractHTTPRequestHandlerTest<T> extends MockableTest {
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IRPCServiceTrafficService trafficService;
    @Mock
    protected IRPCServiceTrafficGovernor trafficGovernor;
    @Mock
    protected IBaseKVMetaService metaService;
    @Mock
    protected IBaseKVClusterMetadataManager metadataManager;

    protected abstract Class<T> handlerClass();

    @BeforeMethod
    @Override
    public void setup() {
        super.setup();
        when(trafficService.getTrafficGovernor(anyString())).thenReturn(trafficGovernor);
        when(metaService.metadataManager(anyString())).thenReturn(metadataManager);
        when(settingProvider.provide(any(), anyString())).thenAnswer(
            invocation -> ((Setting) invocation.getArgument(0)).current(invocation.getArgument(1)));
    }

    @Test
    public final void annotationAttached() {
        assertNotNull(handlerClass().getAnnotation(Path.class));
    }

    protected DefaultFullHttpRequest buildRequest(HttpMethod method) {
        return buildRequest(method, Unpooled.EMPTY_BUFFER);
    }

    protected DefaultFullHttpRequest buildRequest(HttpMethod method, ByteBuf content) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method,
            handlerClass().getAnnotation(Path.class).value(), content);
    }
}
