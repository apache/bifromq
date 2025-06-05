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

package org.apache.bifromq.mqtt.handler;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import org.apache.bifromq.mqtt.MockableTest;
import org.apache.bifromq.mqtt.handler.condition.Condition;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.channelclosed.ChannelError;
import io.netty.channel.embedded.EmbeddedChannel;
import org.mockito.Mock;
import org.testng.annotations.Test;

public class ConditionalRejectHandlerTest extends MockableTest {
    @Mock
    private Condition rejectCondition;
    @Mock
    private IEventCollector eventCollector;

    @Test
    public void testReject() {
        when(rejectCondition.meet()).thenReturn(true);
        EmbeddedChannel embeddedChannel =
            new EmbeddedChannel(new ConditionalRejectHandler(rejectCondition, eventCollector));
        assertFalse(embeddedChannel.isOpen());
        verify(eventCollector).report(argThat(e -> e instanceof ChannelError));
    }
}
