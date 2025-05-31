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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceLandscape;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import io.reactivex.rxjava3.core.Observable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class APIServerTest extends MockableTest {
    private final String host = "127.0.0.1";
    private APIServer apiServer;
    @Mock
    private IAgentHost agentHost;
    @Mock
    private IRPCServiceTrafficGovernor trafficGovernor;
    @Mock
    private IRPCServiceLandscape serviceLandscape;
    @Mock
    private IRPCServiceTrafficService trafficService;
    @Mock
    private IBaseKVMetaService metaService;
    @Mock
    private IDistClient distClient;
    @Mock
    private IInboxClient inboxClient;
    @Mock
    private ISessionDictClient sessionDictClient;
    @Mock
    private IRetainClient retainClient;
    private final ISettingProvider settingProvider = Setting::current;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        super.setup();
        when(metaService.clusterIds()).thenReturn(Observable.empty());
        when(trafficService.services()).thenReturn(Observable.just(Set.of("test_service")));
        when(trafficService.getServiceLandscape(anyString())).thenReturn(serviceLandscape);
        when(trafficService.getTrafficGovernor(anyString())).thenReturn(trafficGovernor);
        when(trafficGovernor.serverEndpoints()).thenReturn(Observable.empty());
        when(trafficGovernor.trafficRules()).thenReturn(Observable.empty());
        apiServer = APIServer.builder()
            .host(host)
            .port(0)
            .maxContentLength(1024 * 1024)
            .agentHost(agentHost)
            .trafficService(trafficService)
            .metaService(metaService)
            .distClient(distClient)
            .inboxClient(inboxClient)
            .sessionDictClient(sessionDictClient)
            .retainClient(retainClient)
            .settingProvider(settingProvider)
            .build();
        apiServer.start();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() {
        super.teardown();
        apiServer.close();
    }

    @Test(groups = "integration")
    @SneakyThrows
    public void pub() {
        HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        HttpRequest pubRequest = HttpRequest.newBuilder()
            .uri(URI.create(String.format("http://%s:%d/pub", host, apiServer.listeningPort())))
            .header(Headers.HEADER_TENANT_ID.header, "BifroMQ-Dev")
            .header(Headers.HEADER_TOPIC.header, "/greeting")
            .header(Headers.HEADER_CLIENT_TYPE.header, "BifroMQ Fan")
            .header(Headers.HEADER_RETAIN.header, "true")
            .header(Headers.HEADER_QOS.header, "1")
            .POST(HttpRequest.BodyPublishers.ofString("Hello BifroMQ"))
            .build();
        when(distClient.pub(anyLong(), anyString(), any(), any())).thenReturn(
            CompletableFuture.completedFuture(PubResult.OK));
        when(retainClient.retain(anyLong(), anyString(), any(), any(), anyInt(), any())).thenReturn(
            CompletableFuture.completedFuture(RetainReply.newBuilder().setResult(RetainReply.Result.RETAINED).build()));
        HttpResponse<?> resp = httpClient.send(pubRequest, HttpResponse.BodyHandlers.discarding());
        assertEquals(resp.statusCode(), 200);
    }
}
