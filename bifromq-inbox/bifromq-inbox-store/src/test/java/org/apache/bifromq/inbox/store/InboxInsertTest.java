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

import static org.apache.bifromq.type.QoS.AT_MOST_ONCE;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchCommitReply;
import org.apache.bifromq.inbox.storage.proto.BatchCommitRequest;
import org.apache.bifromq.inbox.storage.proto.BatchFetchRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSubRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.InsertRequest;
import org.apache.bifromq.inbox.storage.proto.InsertResult;
import org.apache.bifromq.inbox.storage.proto.SubMessagePack;
import org.apache.bifromq.plugin.eventcollector.inboxservice.Overflowed;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicFilterOption;
import org.apache.bifromq.type.TopicMessagePack;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InboxInsertTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void insertNoInbox() {
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        InsertResult insertResult = requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 1L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(message(AT_MOST_ONCE, "hello"))
                    .build())
                .build())
            .build()).get(0);
        assertEquals(insertResult.getCode(), InsertResult.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void commitNoInbox() {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        BatchCommitReply.Code commitCode = requestCommit(BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder().build())
            .setQos0UpToSeq(1)
            .setNow(now)
            .build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.NO_INBOX);
    }

    protected void fetchWithoutStartAfter(QoS qos) {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(2)
            .setLimit(10)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setIncarnation(1L).setQos(qos).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg1 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg2 = message(qos, "world");
        InsertResult insertResult = requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 1L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build()).get(0);
        assertEquals(insertResult.getCode(), InsertResult.Code.OK);
        assertEquals(insertResult.getResult(0).getTopicFilter(), topicFilter);
        assertEquals(insertResult.getResult(0).getIncarnation(), 1L);

        Fetched fetched = requestFetch(
            BatchFetchRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setMaxFetch(10)
                .build())
            .get(0);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg1.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg2.getMessage(0));

        Fetched fetched1 = requestFetch(
            BatchFetchRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setMaxFetch(10)
                .build())
            .get(0);
        assertEquals(fetched, fetched1);
    }

    protected void fetchWithMaxLimit(QoS qos) {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setExpirySeconds(2)
            .setLimit(10)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setIncarnation(1L).setQos(qos).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build());
        TopicMessagePack.PublisherPack msg1 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg2 = message(qos, "world");
        InsertResult insertResult = requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 1L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build()).get(0);
        assertEquals(insertResult.getCode(), InsertResult.Code.OK);
        assertEquals(insertResult.getResult(0).getTopicFilter(), topicFilter);
        assertEquals(insertResult.getResult(0).getIncarnation(), 1L);

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(1)
            .build())
            .get(0);
        assertEquals(msgCountGetter(qos).apply(fetched), 1);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg1.getMessage(0));

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(msgCountGetter(qos).apply(fetched), 2);

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(0)
            .build())
            .get(0);
        assertEquals(msgCountGetter(qos).apply(fetched), 0);
    }

    protected void fetchWithStartAfter(QoS qos) {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setExpirySeconds(2)
            .setLimit(10)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setQos(qos).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg1 = message(qos, "a");
        TopicMessagePack.PublisherPack msg2 = message(qos, "b");
        TopicMessagePack.PublisherPack msg3 = message(qos, "c");
        TopicMessagePack.PublisherPack msg4 = message(qos, "d");
        TopicMessagePack.PublisherPack msg5 = message(qos, "e");
        TopicMessagePack.PublisherPack msg6 = message(qos, "f");
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .addMessage(msg3)
                    .build())
                .build())
            .build());
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg4)
                    .addMessage(msg5)
                    .addMessage(msg6)
                    .build())
                .build())
            .build());

        BatchFetchRequest.Params.Builder paramsBuilder = BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(1);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0StartAfter(0);
            case AT_LEAST_ONCE, EXACTLY_ONCE -> paramsBuilder.setSendBufferStartAfter(0);
        }
        Fetched fetched = requestFetch(paramsBuilder.build()).get(0);

        if (qos == AT_MOST_ONCE) {
            assertEquals(msgCountGetter(qos).apply(fetched), 5);
        } else {
            // limit only apply buffered messages
            assertEquals(msgCountGetter(qos).apply(fetched), 1);
            assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg2.getMessage(0));
        }

        paramsBuilder = BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0StartAfter(0);
            case AT_LEAST_ONCE, EXACTLY_ONCE -> paramsBuilder.setSendBufferStartAfter(0);
        }

        fetched = requestFetch(paramsBuilder.build()).get(0);
        assertEquals(msgCountGetter(qos).apply(fetched), 5);
        assertEquals(msgGetter(qos).apply(fetched, 0).getSeq(), 1);
        assertEquals(msgGetter(qos).apply(fetched, 1).getSeq(), 2);
        assertEquals(msgGetter(qos).apply(fetched, 2).getSeq(), 3);
        assertEquals(msgGetter(qos).apply(fetched, 3).getSeq(), 4);
        assertEquals(msgGetter(qos).apply(fetched, 4).getSeq(), 5);

        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg2.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg3.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 2).getMsg().getMessage(), msg4.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 3).getMsg().getMessage(), msg5.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 4).getMsg().getMessage(), msg6.getMessage(0));

        paramsBuilder = BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0StartAfter(5);
            case AT_LEAST_ONCE, EXACTLY_ONCE -> paramsBuilder.setSendBufferStartAfter(5);
        }
        fetched = requestFetch(paramsBuilder.build()).get(0);
        assertEquals(msgCountGetter(qos).apply(fetched), 0);
    }

    protected void commit(QoS qos) {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setExpirySeconds(2)
            .setLimit(10)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setQos(qos).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build());
        TopicMessagePack.PublisherPack msg1 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg2 = message(qos, "world");
        TopicMessagePack.PublisherPack msg3 = message(qos, "!!!!!");

        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .addMessage(msg3)
                    .build())
                .build())
            .build());

        BatchCommitRequest.Params.Builder paramsBuilder = BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setNow(now);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0UpToSeq(1);
            case AT_LEAST_ONCE, EXACTLY_ONCE -> paramsBuilder.setSendBufferUpToSeq(1);
        }
        BatchCommitReply.Code commitCode = requestCommit(paramsBuilder.build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.OK);

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(msgCountGetter(qos).apply(fetched), 1);
        assertEquals(msgGetter(qos).apply(fetched, 0).getSeq(), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg3.getMessage(0));

        // nothing should happen
        paramsBuilder = BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setNow(now);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0UpToSeq(1);
            case AT_LEAST_ONCE, EXACTLY_ONCE -> paramsBuilder.setSendBufferUpToSeq(1);
        }
        commitCode = requestCommit(paramsBuilder.build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.OK);

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);

        assertEquals(msgCountGetter(qos).apply(fetched), 1);
        assertEquals(msgGetter(qos).apply(fetched, 0).getSeq(), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg3.getMessage(0));

        paramsBuilder = BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setNow(now);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0UpToSeq(2);
            case AT_LEAST_ONCE, EXACTLY_ONCE -> paramsBuilder.setSendBufferUpToSeq(2);
        }
        commitCode = requestCommit(paramsBuilder.build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.OK);
        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(msgCountGetter(qos).apply(fetched), 0);
    }

    protected void commitAll(QoS qos) {
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setExpirySeconds(2)
            .setLimit(10)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setQos(qos).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg1 = message(qos, "a");
        TopicMessagePack.PublisherPack msg2 = message(qos, "b");
        TopicMessagePack.PublisherPack msg3 = message(qos, "c");
        TopicMessagePack.PublisherPack msg4 = message(qos, "d");
        TopicMessagePack.PublisherPack msg5 = message(qos, "e");
        TopicMessagePack.PublisherPack msg6 = message(qos, "f");
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .addMessage(msg3)
                    .build())
                .build())
            .build());
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg4)
                    .addMessage(msg5)
                    .addMessage(msg6)
                    .build())
                .build())
            .build());

        BatchCommitRequest.Params.Builder paramsBuilder = BatchCommitRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setNow(now);
        switch (qos) {
            case AT_MOST_ONCE -> paramsBuilder.setQos0UpToSeq(5);
            case AT_LEAST_ONCE, EXACTLY_ONCE -> paramsBuilder.setSendBufferUpToSeq(5);
        }
        BatchCommitReply.Code commitCode = requestCommit(paramsBuilder.build()).get(0);
        assertEquals(commitCode, BatchCommitReply.Code.OK);

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(msgCountGetter(qos).apply(fetched), 0);
    }

    protected void insertDropOldest(QoS qos) {
        clearInvocations(eventCollector);
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(2)
            .setDropOldest(true)
            .setLimit(2)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setQos(qos).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg0 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg1 = message(qos, "world");
        TopicMessagePack.PublisherPack msg2 = message(qos, "!!!!!");
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .build())
                .build())
            .build());
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .build())
                .build())
            .build());
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg2)
                    .build())
                .build())
            .build());

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector).report(argCap.capture());
        Overflowed event = argCap.getValue();
        assertTrue(event.oldest());
        assertEquals(event.isQoS0(), qos == AT_MOST_ONCE);
        assertEquals(event.dropCount(), 1);

        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getSeq(), 1);
        assertEquals(msgGetter(qos).apply(fetched, 1).getSeq(), 2);

        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg1.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg2.getMessage(0));

        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build());

        verify(eventCollector, times(2)).report(argCap.capture());
        event = argCap.getValue();
        assertTrue(event.oldest());
        assertEquals(event.isQoS0(), qos == AT_MOST_ONCE);
        assertEquals(event.dropCount(), 3);

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getSeq(), 4);
        assertEquals(msgGetter(qos).apply(fetched, 1).getSeq(), 5);

        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg1.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg2.getMessage(0));
    }

    protected void insertDropYoungest(QoS qos) {
        clearInvocations(eventCollector);
        long now = 0;
        String tenantId = "tenantId-" + System.nanoTime();
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        String topicFilter = "/a/b/c";
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(2)
            .setDropOldest(false)
            .setLimit(2)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setQos(qos).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build());

        TopicMessagePack.PublisherPack msg0 = message(qos, "hello");
        TopicMessagePack.PublisherPack msg1 = message(qos, "world");
        TopicMessagePack.PublisherPack msg2 = message(qos, "!!!!!");
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .build())
                .build())
            .build());
        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build());
        Fetched fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);

        assertEquals(msgGetter(qos).apply(fetched, 0).getSeq(), 0);
        assertEquals(msgGetter(qos).apply(fetched, 1).getSeq(), 1);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg0.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg1.getMessage(0));

        requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build());

        ArgumentCaptor<Overflowed> argCap = ArgumentCaptor.forClass(Overflowed.class);
        verify(eventCollector, times(2)).report(argCap.capture());
        for (Overflowed event : argCap.getAllValues()) {
            assertFalse(event.oldest());
            assertEquals(event.isQoS0(), qos == AT_MOST_ONCE);
            assertTrue(event.dropCount() == 1 || event.dropCount() == 3);
        }

        fetched = requestFetch(BatchFetchRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setMaxFetch(10)
            .build())
            .get(0);
        assertEquals(msgGetter(qos).apply(fetched, 0).getSeq(), 0);
        assertEquals(msgGetter(qos).apply(fetched, 1).getSeq(), 1);

        assertEquals(msgCountGetter(qos).apply(fetched), 2);
        assertEquals(msgGetter(qos).apply(fetched, 0).getMsg().getMessage(), msg0.getMessage(0));
        assertEquals(msgGetter(qos).apply(fetched, 1).getMsg().getMessage(), msg1.getMessage(0));
    }

    @Test(groups = "integration")
    public void insertQoS012() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        String topicFilter = "greeting";
        TopicMessagePack.PublisherPack msg0 = message(QoS.AT_MOST_ONCE, "hello");
        TopicMessagePack.PublisherPack msg1 = message(QoS.AT_LEAST_ONCE, "world");
        TopicMessagePack.PublisherPack msg2 = message(QoS.EXACTLY_ONCE, "!!!!!");
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setDropOldest(false)
            .setLimit(3)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        requestSub(BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setOption(TopicFilterOption.newBuilder().setQos(QoS.EXACTLY_ONCE).build())
            .setMaxTopicFilters(100)
            .setNow(now)
            .build());
        InsertResult insertResult = requestInsert(InsertRequest.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .addMessagePack(SubMessagePack.newBuilder()
                .putMatchedTopicFilters(topicFilter, 0L)
                .setMessages(TopicMessagePack.newBuilder()
                    .setTopic(topicFilter)
                    .addMessage(msg0)
                    .addMessage(msg1)
                    .addMessage(msg2)
                    .build())
                .build())
            .build())
            .get(0);
        assertEquals(insertResult.getCode(), InsertResult.Code.OK);
        Fetched fetched = requestFetch(
            BatchFetchRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setMaxFetch(10)
                .build())
            .get(0);
        Assert.assertEquals(fetched.getQos0MsgCount(), 1);
        Assert.assertEquals(fetched.getQos0Msg(0).getMsg().getMessage(), msg0.getMessage(0));

        Assert.assertEquals(fetched.getSendBufferMsgCount(), 2);
        Assert.assertEquals(fetched.getSendBufferMsg(0).getSeq(), 0);
        Assert.assertEquals(fetched.getSendBufferMsg(1).getSeq(), 1);
        Assert.assertEquals(fetched.getSendBufferMsg(0).getMsg().getMessage(), msg1.getMessage(0));
        Assert.assertEquals(fetched.getSendBufferMsg(1).getMsg().getMessage(), msg2.getMessage(0));
    }

    private Function<Fetched, Integer> msgCountGetter(QoS qos) {
        return switch (qos) {
            case AT_MOST_ONCE -> Fetched::getQos0MsgCount;
            default -> Fetched::getSendBufferMsgCount;
        };
    }

    private BiFunction<Fetched, Integer, InboxMessage> msgGetter(QoS qos) {
        return switch (qos) {
            case AT_MOST_ONCE -> Fetched::getQos0Msg;
            default -> Fetched::getSendBufferMsg;
        };
    }
}

