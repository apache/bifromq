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

import static org.apache.bifromq.apiserver.Headers.HEADER_BALANCER_FACTORY_CLASS;
import static org.apache.bifromq.apiserver.Headers.HEADER_STORE_NAME;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetLoadRulesHandlerTest extends AbstractHTTPRequestHandlerTest<GetLoadRulesHandler> {

    private final Subject<Map<String, BalancerStateSnapshot>> mockBalancerStatesSubject = BehaviorSubject.create();
    private final Subject<Set<String>> mockClusterIdSubject = BehaviorSubject.create();

    @BeforeMethod
    public void setup() {
        super.setup();
        when(metaService.clusterIds()).thenReturn(mockClusterIdSubject);
    }

    @Override
    protected Class<GetLoadRulesHandler> handlerClass() {
        return GetLoadRulesHandler.class;
    }

    @Test
    public void noClusterFound() {
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        req.headers().set(HEADER_STORE_NAME.header, "fakeUserId");
        req.headers().set(HEADER_BALANCER_FACTORY_CLASS.header, "fakeBalancerFactoryClass");
        GetLoadRulesHandler handler = new GetLoadRulesHandler(metaService);
        FullHttpResponse resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.NOT_FOUND);
    }

    @Test
    public void clusterChanged() {
        String clusterId = "dist.worker";
        String balancerFacClass = "balancerFactoryClass";
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        req.headers().set(HEADER_STORE_NAME.header, clusterId);
        req.headers().set(HEADER_BALANCER_FACTORY_CLASS.header, balancerFacClass);
        when(statesProposal.expectedBalancerStates()).thenReturn(mockBalancerStatesSubject);

        GetLoadRulesHandler handler = new GetLoadRulesHandler(metaService);
        handler.start();
        FullHttpResponse resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.NOT_FOUND);

        mockClusterIdSubject.onNext(Set.of(clusterId));
        resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.OK);

        mockClusterIdSubject.onNext(Collections.emptySet());
        resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.NOT_FOUND);
    }

    @Test
    public void loadRules() {
        String storeName = "dist.worker";
        String balancerFacClass = "balancerFactoryClass";
        DefaultFullHttpRequest req = buildRequest(HttpMethod.GET);
        req.headers().set(HEADER_STORE_NAME.header, storeName);
        req.headers().set(HEADER_BALANCER_FACTORY_CLASS.header, balancerFacClass);
        when(statesProposal.expectedBalancerStates()).thenReturn(mockBalancerStatesSubject);
        mockClusterIdSubject.onNext(Set.of(storeName));

        Map<String, BalancerStateSnapshot> expected = Map.of("balancerFactoryClass",
            BalancerStateSnapshot.getDefaultInstance());
        mockBalancerStatesSubject.onNext(expected);
        GetLoadRulesHandler handler = new GetLoadRulesHandler(metaService);
        handler.start();
        FullHttpResponse resp = handler.handle(123, req).join();
        assertEquals(resp.status(), HttpResponseStatus.OK);
    }
}
