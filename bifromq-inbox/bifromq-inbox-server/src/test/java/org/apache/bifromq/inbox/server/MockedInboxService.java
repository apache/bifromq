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

package org.apache.bifromq.inbox.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.baserpc.RPCContext;
import org.apache.bifromq.baserpc.metrics.IRPCMeter;
import org.apache.bifromq.baserpc.metrics.RPCMetric;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.server.scheduler.IInboxAttachScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxCheckSubScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxCommitScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxDeleteScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxDetachScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxExistScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxFetchScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxFetchStateScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxSubScheduler;
import org.apache.bifromq.inbox.server.scheduler.IInboxUnsubScheduler;
import org.apache.bifromq.inbox.util.PipelineUtil;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.retain.client.IRetainClient;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class MockedInboxService {
    @Mock
    protected ISettingProvider settingProvider;
    @Mock
    protected IEventCollector eventCollector;
    @Mock
    protected IInboxClient inboxClient;
    @Mock
    protected IDistClient distClient;
    @Mock
    protected IRetainClient retainClient;
    @Mock
    protected IBaseKVStoreClient inboxStoreClient;
    @Mock
    protected IInboxFetchScheduler fetchScheduler;
    @Mock
    protected IInboxFetchStateScheduler fetchStateScheduler;
    @Mock
    protected IInboxExistScheduler existScheduler;
    @Mock
    protected IInboxCheckSubScheduler checkSubScheduler;
    @Mock
    protected IInboxInsertScheduler insertScheduler;
    @Mock
    protected IInboxCommitScheduler commitScheduler;
    @Mock
    protected IInboxAttachScheduler attachScheduler;
    @Mock
    protected IInboxDetachScheduler detachScheduler;
    @Mock
    protected IInboxDeleteScheduler deleteScheduler;
    @Mock
    protected IInboxSubScheduler subScheduler;
    @Mock
    protected IInboxUnsubScheduler unsubScheduler;

    protected String tenantId = "testTenantId";
    protected String serviceName = "inboxService";
    protected String methodName = "testMethod";
    protected InboxService inboxService;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(inboxClient.id()).thenReturn(1);
        when(settingProvider.provide(any(), anyString())).thenAnswer(invocation -> {
            Setting setting = invocation.getArgument(0);
            return setting.current(invocation.getArgument(1));
        });
        Map<String, String> metaData = new HashMap<>();
        metaData.put(PipelineUtil.PIPELINE_ATTR_KEY_ID, "id");
        Context.current()
            .withValue(RPCContext.METER_KEY_CTX_KEY, new IRPCMeter.IRPCMethodMeter() {
                @Override
                public void recordCount(RPCMetric metric) {

                }

                @Override
                public void recordCount(RPCMetric metric, double inc) {

                }

                @Override
                public Timer timer(RPCMetric metric) {
                    return Timer.builder("dummy").register(new SimpleMeterRegistry());
                }

                @Override
                public void recordSummary(RPCMetric metric, int depth) {

                }
            })
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metaData)
            .attach();
        inboxService = InboxService.builder()
            .inboxClient(inboxClient)
            .distClient(distClient)
            .fetchStateScheduler(fetchStateScheduler)
            .existScheduler(existScheduler)
            .checkSubScheduler(checkSubScheduler)
            .fetchScheduler(fetchScheduler)
            .insertScheduler(insertScheduler)
            .commitScheduler(commitScheduler)
            .attachScheduler(attachScheduler)
            .detachScheduler(detachScheduler)
            .deleteScheduler(deleteScheduler)
            .subScheduler(subScheduler)
            .unsubScheduler(unsubScheduler)
            .build();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        closeable.close();
    }
}
