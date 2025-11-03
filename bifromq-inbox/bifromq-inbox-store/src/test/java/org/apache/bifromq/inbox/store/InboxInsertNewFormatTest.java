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
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchFetchRequest;
import org.apache.bifromq.inbox.storage.proto.BatchInsertRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.InsertResult;
import org.apache.bifromq.inbox.storage.proto.MatchedRoute;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.type.TopicMessagePack;
import org.testng.annotations.Test;

public class InboxInsertNewFormatTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void insertWithMessagePool() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topic = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        InboxVersion version = requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(10)
            .setLimit(10)
            .setClient(client)
            .setNow(now)
            .build()).get(0);

        requestSub(org.apache.bifromq.inbox.storage.proto.BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(version)
            .setTopicFilter(topic)
            .setOption(TopicFilterOption.newBuilder().setIncarnation(1L).setQos(QoS.AT_LEAST_ONCE).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack pubPack = TopicMessagePack.PublisherPack.newBuilder()
            .addMessage(Message.newBuilder().setMessageId(System.nanoTime()).setPubQoS(QoS.AT_LEAST_ONCE).build())
            .build();
        TopicMessagePack topicMessagePack = TopicMessagePack.newBuilder().setTopic(topic).addMessage(pubPack).build();

        BatchInsertRequest.SubRef subRef = BatchInsertRequest.SubRef.newBuilder()
            .addMatchedRoute(MatchedRoute.newBuilder().setTopicFilter(topic).setIncarnation(1L).build())
            .setMessagePackIndex(0)
            .build();

        BatchInsertRequest.InsertRef insertRef = BatchInsertRequest.InsertRef.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addSubRef(subRef)
            .build();

        BatchInsertRequest req = BatchInsertRequest.newBuilder()
            .addTopicMessagePack(topicMessagePack)
            .addInsertRef(insertRef)
            .build();

        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix(tenantId, inboxId);
        InboxServiceRWCoProcInput input = MessageUtil.buildInsertRequest(reqId, req);
        InboxServiceRWCoProcOutput output = mutate(routeKey, input);
        assertTrue(output.hasBatchInsert());
        assertEquals(output.getReqId(), reqId);
        List<InsertResult> results = output.getBatchInsert().getResultList();
        assertEquals(results.size(), 1);
        assertEquals(results.get(0).getCode(), InsertResult.Code.OK);

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build()).get(0);
        if (fetched.getResult() == Fetched.Result.OK) {
            assertEquals(fetched.getSendBufferMsgCount(), 1);
            assertEquals(fetched.getSendBufferMsg(0).getMsg().getMessage(), pubPack.getMessage(0));
        }
    }
}

