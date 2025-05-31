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

package org.apache.bifromq.plugin.subbroker;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.TopicMessagePack;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SubBrokerManagerTest {
    private AutoCloseable closeable;
    @Mock
    private PluginManager plugin;

    @Mock
    private ISubBroker subBroker1;
    @Mock
    private IDeliverer deliverer1;
    @Mock
    private ISubBroker subBroker2;
    @Mock
    private IDeliverer deliverer2;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void subBrokerIdConflict() {
        when(subBroker1.id()).thenReturn(0);
        when(subBroker1.open(anyString())).thenReturn(deliverer1);
        when(subBroker2.id()).thenReturn(0);
        when(plugin.getExtensions(ISubBroker.class)).thenReturn(List.of(subBroker1, subBroker2));
        SubBrokerManager subBrokerManager = new SubBrokerManager(plugin);
        ISubBroker subBroker = subBrokerManager.get(0);
        IDeliverer deliverer = subBroker.open("Deliverer1");
        deliverer.close();
        verify(deliverer1, times(1)).close();
    }

    @Test
    public void noSubBroker() {
        when(subBroker1.id()).thenReturn(0);
        when(subBroker1.open(anyString())).thenReturn(deliverer1);
        when(plugin.getExtensions(ISubBroker.class)).thenReturn(List.of(subBroker1));
        String tenantId = "tenant";
        SubBrokerManager subBrokerManager = new SubBrokerManager(plugin);
        ISubBroker subBroker = subBrokerManager.get(1);
        IDeliverer deliverer = subBroker.open("Deliverer1");
        TopicMessagePack msgPack = TopicMessagePack.newBuilder().build();
        MatchInfo matchInfo = MatchInfo.newBuilder().build();
        DeliveryRequest request = DeliveryRequest.newBuilder()
            .putPackage(tenantId, DeliveryPackage.newBuilder()
                .addPack(DeliveryPack.newBuilder()
                    .setMessagePack(msgPack)
                    .addMatchInfo(matchInfo)
                    .build())
                .build())
            .build();
        DeliveryReply reply = deliverer.deliver(request).join();
        assertEquals(reply.getResultMap().get(tenantId).getResult(0).getCode(), DeliveryResult.Code.NO_SUB);
        assertEquals(reply.getResultMap().get(tenantId).getResult(0).getMatchInfo(), matchInfo);
    }

    @Test
    public void close() {
        when(subBroker1.id()).thenReturn(0);
        when(subBroker2.id()).thenReturn(1);
        when(plugin.getExtensions(ISubBroker.class)).thenReturn(List.of(subBroker1, subBroker2));
        SubBrokerManager subBrokerManager = new SubBrokerManager(plugin);
        subBrokerManager.close();
        verify(subBroker1, times(1)).close();
        verify(subBroker2, times(1)).close();
    }
}
