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

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.clientdisconnect.ResourceThrottled;
import org.apache.bifromq.sysprops.props.MaxSlowDownTimeoutSeconds;
import org.apache.bifromq.type.ClientInfo;
import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class ConditionalSlowDownHandlerTest extends MockableTest {
    @Mock
    Supplier<Long> nanoProvider;
    @Mock
    private Condition slowDownCondition;
    @Mock
    private IEventCollector eventCollector;
    private ClientInfo clientInfo = ClientInfo.newBuilder().build();

    @Test
    public void testAutoReadDisable() {
        when(slowDownCondition.meet()).thenReturn(true);
        when(nanoProvider.get()).thenReturn(0L);
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            new ConditionalSlowDownHandler(slowDownCondition, eventCollector, nanoProvider, clientInfo));
        embeddedChannel.writeInbound("1");
        assertEquals(embeddedChannel.readInbound(), "1");
        assertFalse(embeddedChannel.config().isAutoRead());

        embeddedChannel.advanceTimeBy(1, TimeUnit.SECONDS);
        when(nanoProvider.get()).thenReturn(Duration.ofSeconds(1).toNanos());
        embeddedChannel.runScheduledPendingTasks();
        assertFalse(embeddedChannel.config().isAutoRead());

        when(slowDownCondition.meet()).thenReturn(false);
        embeddedChannel.advanceTimeBy(1, TimeUnit.SECONDS);
        when(nanoProvider.get()).thenReturn(Duration.ofSeconds(2).toNanos());
        embeddedChannel.runScheduledPendingTasks();
        assertTrue(embeddedChannel.config().isAutoRead());
    }

    @Test
    public void testSlowDownTimeout() {
        when(slowDownCondition.meet()).thenReturn(true);
        when(slowDownCondition.toString()).thenReturn("slowDownCondition");
        when(nanoProvider.get()).thenReturn(0L);
        EmbeddedChannel embeddedChannel =
            new EmbeddedChannel(
                new ConditionalSlowDownHandler(slowDownCondition, eventCollector, nanoProvider,
                    clientInfo));
        embeddedChannel.writeInbound("1");
        assertEquals(embeddedChannel.readInbound(), "1");
        assertFalse(embeddedChannel.config().isAutoRead());

        embeddedChannel.advanceTimeBy(1, TimeUnit.SECONDS);
        embeddedChannel.runScheduledPendingTasks();
        assertFalse(embeddedChannel.config().isAutoRead());

        embeddedChannel.advanceTimeBy(MaxSlowDownTimeoutSeconds.INSTANCE.get(), TimeUnit.SECONDS);
        long now = Duration.ofSeconds(MaxSlowDownTimeoutSeconds.INSTANCE.get() + 1).toNanos();
        when(nanoProvider.get()).thenReturn(now);
        embeddedChannel.runScheduledPendingTasks();
        assertFalse(embeddedChannel.isOpen());
        verify(eventCollector).report(argThat(event -> event instanceof ResourceThrottled &&
            ((ResourceThrottled) event).reason().equals("slowDownCondition")));
    }
}
