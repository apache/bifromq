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

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.reactivex.rxjava3.core.Observable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AbstractTrafficRulesHandlerTest {

    @Mock
    private IRPCServiceTrafficService mockTrafficService;

    @Mock
    private IRPCServiceTrafficGovernor mockGovernor;

    private AbstractTrafficRulesHandler handler;

    @BeforeMethod
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Initialize the AbstractTrafficRulesHandler instance
        handler = new AbstractTrafficRulesHandler(mockTrafficService) {
            @Override
            public CompletableFuture<FullHttpResponse> handle(long reqId, FullHttpRequest req) {
                return null;
            }
        };
    }

    @Test
    void startAndSubscribeServices() {
        Set<String> services = Set.of(
            "distservice.DistService",
            "inboxservice.InboxService",
            "sessiondict.SessionDictService",
            "retainservice.RetainService",
            "mqttbroker.BrokerService"
        );

        when(mockTrafficService.services()).thenReturn(Observable.just(services));
        when(mockTrafficService.getTrafficGovernor(anyString())).thenReturn(mockGovernor);

        handler.start();
        verify(mockTrafficService, times(1)).getTrafficGovernor("distservice.DistService");
        verify(mockTrafficService, times(1)).getTrafficGovernor("inboxservice.InboxService");
        verify(mockTrafficService, times(1)).getTrafficGovernor("sessiondict.SessionDictService");
        verify(mockTrafficService, times(1)).getTrafficGovernor("retainservice.RetainService");
        verify(mockTrafficService, times(1)).getTrafficGovernor("mqttbroker.BrokerService");

        assertTrue(handler.governorMap.containsKey("DistService"));
        assertTrue(handler.governorMap.containsKey("InboxService"));
        assertTrue(handler.governorMap.containsKey("SessionDictService"));
        assertTrue(handler.governorMap.containsKey("RetainService"));
        assertTrue(handler.governorMap.containsKey("BrokerService"));
        verify(mockTrafficService, times(1)).services();
    }
}