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

package org.apache.bifromq.mqtt.service;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.session.IMQTTSession;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class LocalSessionRegistryTest extends MockableTest {
    @Mock
    private IMQTTSession session1;
    @Mock
    private IMQTTSession session2;

    @Test
    public void testAdd() {
        LocalSessionRegistry localSessionRegistry = new LocalSessionRegistry();
        localSessionRegistry.add("sessionId", session1);
        assertEquals(session1, localSessionRegistry.get("sessionId"));
    }

    @Test
    public void testRemove() {
        LocalSessionRegistry localSessionRegistry = new LocalSessionRegistry();
        localSessionRegistry.add("sessionId", session1);
        localSessionRegistry.remove("sessionId", session1);
        assertNull(localSessionRegistry.get("sessionId"));
    }

    @Test
    public void testDisconnectAll() {
        when(session1.disconnect()).thenReturn(CompletableFuture.completedFuture(null));
        when(session2.disconnect()).thenReturn(CompletableFuture.completedFuture(null));
        LocalSessionRegistry localSessionRegistry = new LocalSessionRegistry();
        localSessionRegistry.add("sessionId1", session1);
        localSessionRegistry.add("sessionId2", session2);
        localSessionRegistry.disconnectAll(1);
        assertNull(localSessionRegistry.get("sessionId1"));
        assertNull(localSessionRegistry.get("sessionId2"));
        verify(session1).disconnect();
        verify(session2).disconnect();
    }
}
