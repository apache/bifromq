/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.mqtt.handler;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.condition.InboundResourceCondition;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.resourcethrottler.TenantResourceType;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class InboundResourceConditionTest extends MockableTest {
    @Mock
    public IResourceThrottler resourceThrottler;
    public ClientInfo clientInfo = ClientInfo.newBuilder().setTenantId("tenantA").build();

    @Test
    public void testNoInboundResource() {
        InboundResourceCondition condition =
            new InboundResourceCondition(resourceThrottler, clientInfo);
        when(resourceThrottler.hasResource(clientInfo.getTenantId(),
            TenantResourceType.TotalInboundBytesPerSecond)).thenReturn(false);
        assertTrue(condition.meet());

        when(resourceThrottler.hasResource(clientInfo.getTenantId(),
            TenantResourceType.TotalInboundBytesPerSecond)).thenReturn(true);
        assertFalse(condition.meet());
    }
}
