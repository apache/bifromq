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

import static org.testng.Assert.assertEquals;

import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteReply;
import org.apache.bifromq.inbox.storage.proto.BatchDeleteRequest;
import org.apache.bifromq.inbox.storage.proto.BatchFetchRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSubRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.InsertRequest;
import org.apache.bifromq.inbox.storage.proto.MatchedRoute;
import org.apache.bifromq.inbox.storage.proto.SubMessagePack;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.type.TopicMessagePack;
import org.testng.annotations.Test;

public class InboxDeleteClearAllTest extends InboxStoreTest {

    @Test(groups = "integration")
    public void clearInboxRemovesAllMessages() {
        String tenantId = "tenant-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();

        InboxVersion version = requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(30)
            .setLimit(1000)
            .setClient(client)
            .setNow(0)
            .build()).get(0);

        // subscribe QoS0 and QoS1 filters
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

        // insert many messages in multiple chunks for both queues
        for (int i = 0; i < 200; i++) {
            requestInsert(InsertRequest.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .addMessagePack(SubMessagePack.newBuilder()
                    .addMatchedRoute(MatchedRoute.newBuilder().setTopicFilter("/qos0").setIncarnation(1L).build())
                    .setMessages(TopicMessagePack.newBuilder()
                        .setTopic("/qos0")
                        .addMessage(message(QoS.AT_MOST_ONCE, "m0-" + i))
                        .build())
                    .build())
                .addMessagePack(SubMessagePack.newBuilder()
                    .addMatchedRoute(MatchedRoute.newBuilder().setTopicFilter("/qos1").setIncarnation(1L).build())
                    .setMessages(TopicMessagePack.newBuilder()
                        .setTopic("/qos1")
                        .addMessage(message(QoS.AT_LEAST_ONCE, "m1-" + i))
                        .build())
                    .build())
                .build());
        }

        // delete inbox
        BatchDeleteReply.Result delResult = requestDelete(BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(version)
            .build()).get(0);
        assertEquals(delResult.getCode(), BatchDeleteReply.Code.OK);

        // fetch after delete should return NO_INBOX
        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(1000)
            .build()).get(0);
        assertEquals(fetched.getResult(), Fetched.Result.NO_INBOX);
    }
}

