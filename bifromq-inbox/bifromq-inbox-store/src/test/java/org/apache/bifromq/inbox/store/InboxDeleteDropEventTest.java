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

package org.apache.bifromq.inbox.store;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteReply;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSubRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.InsertRequest;
import org.apache.bifromq.inbox.storage.proto.MatchedRoute;
import org.apache.bifromq.inbox.storage.proto.SubMessagePack;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.DropReason;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS0Dropped;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS1Dropped;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling.QoS2Dropped;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.type.TopicMessagePack;
import org.testng.annotations.Test;

public class InboxDeleteDropEventTest extends InboxStoreTest {

    @Test(groups = "integration")
    public void dropEventsOnDelete() {
        String tenantId = "tenant-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();

        InboxVersion version = requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(30)
            .setLimit(100)
            .setClient(client)
            .setNow(0)
            .build()).get(0);

        // subscribe three topic filters
        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(version)
            .setTopicFilter("/qos0")
            .setOption(TopicFilterOption.newBuilder().setIncarnation(1L).setQos(QoS.AT_MOST_ONCE).build())
            .setMaxTopicFilters(100)
            .setNow(0)
            .build());

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(version)
            .setTopicFilter("/qos1")
            .setOption(TopicFilterOption.newBuilder().setIncarnation(1L).setQos(QoS.AT_LEAST_ONCE).build())
            .setMaxTopicFilters(100)
            .setNow(0)
            .build());

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(version)
            .setTopicFilter("/qos2")
            .setOption(TopicFilterOption.newBuilder().setIncarnation(1L).setQos(QoS.EXACTLY_ONCE).build())
            .setMaxTopicFilters(100)
            .setNow(0)
            .build());

        // insert messages for each topic
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .addMatchedRoute(MatchedRoute.newBuilder().setTopicFilter("/qos0").setIncarnation(1L).build())
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic("/qos0")
                    .addMessage(message(QoS.AT_MOST_ONCE, "m0"))
                    .build())
                .build())
            .addMessagePack(SubMessagePack.newBuilder()
                .addMatchedRoute(MatchedRoute.newBuilder().setTopicFilter("/qos1").setIncarnation(1L).build())
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic("/qos1")
                    .addMessage(message(QoS.EXACTLY_ONCE, "m1"))
                    .build())
                .build())
            .addMessagePack(SubMessagePack.newBuilder()
                .addMatchedRoute(MatchedRoute.newBuilder().setTopicFilter("/qos2").setIncarnation(1L).build())
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic("/qos2")
                    .addMessage(message(QoS.EXACTLY_ONCE, "m2"))
                    .build())
                .build())
            .build());

        reset(eventCollector);

        BatchDeleteReply.Result delResult = requestDelete(BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(version)
            .build()).get(0);
        assertEquals(delResult.getCode(), BatchDeleteReply.Code.OK);

        // verify drop events reported on leader after delete
        verify(eventCollector, timeout(4000))
            .report(argThat(e -> e instanceof QoS0Dropped d && d.reason() == DropReason.SessionClosed));
        verify(eventCollector, timeout(4000))
            .report(argThat(e -> e instanceof QoS1Dropped d && d.reason() == DropReason.SessionClosed));
        verify(eventCollector, timeout(4000))
            .report(argThat(e -> e instanceof QoS2Dropped d && d.reason() == DropReason.SessionClosed));
    }

    @Test(groups = "integration")
    public void noDropEventWhenNoInbox() {
        String tenantId = "tenant-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();

        reset(eventCollector);

        BatchDeleteReply.Result delResult = requestDelete(BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.getDefaultInstance())
            .build()).get(0);
        assertEquals(delResult.getCode(), BatchDeleteReply.Code.NO_INBOX);

        verify(eventCollector, timeout(1000).times(0))
            .report(argThat(e -> e instanceof QoS0Dropped
                || e instanceof QoS1Dropped
                || e instanceof QoS2Dropped));
    }
}

