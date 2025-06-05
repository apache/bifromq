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
import org.apache.bifromq.inbox.storage.proto.BatchSubReply;
import org.apache.bifromq.inbox.storage.proto.BatchSubRequest;
import org.apache.bifromq.inbox.storage.proto.BatchUnsubReply;
import org.apache.bifromq.inbox.storage.proto.BatchUnsubRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.type.ClientInfo;
import java.util.Set;
import org.testng.annotations.Test;

public class InboxSubUnsubTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void subNoInbox() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder().build())
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void subConflict() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion.toBuilder().setMod(inboxVersion.getMod() + 1).build())
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void sub() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(10)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.OK);

        subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build();
        code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.EXISTS);
    }

    @Test(groups = "integration")
    public void subAndDelete() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.OK);

        BatchDeleteRequest.Params deleteParams = BatchDeleteRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .build();
        BatchDeleteReply.Result result = requestDelete(deleteParams).get(0);
        assertEquals(result.getCode(), BatchDeleteReply.Code.OK);
        assertEquals(result.getTopicFiltersMap().keySet(), Set.of(topicFilter));
    }

    @Test(groups = "integration")
    public void unsubNoInbox() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder().build())
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchUnsubReply.Result result = requestUnsub(unsubParams).get(0);
        assertEquals(result.getCode(), BatchUnsubReply.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void unsubConflict() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion.toBuilder().setMod(inboxVersion.getMod() + 1).build())
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchUnsubReply.Result result = requestUnsub(unsubParams).get(0);
        assertEquals(result.getCode(), BatchUnsubReply.Code.CONFLICT);
    }

    @Test(groups = "integration")
    public void unsubNoSub() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchUnsubReply.Result result = requestUnsub(unsubParams).get(0);
        assertEquals(result.getCode(), BatchUnsubReply.Code.NO_SUB);
    }

    @Test(groups = "integration")
    public void unsub() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(10)
            .setNow(now)
            .build();
        requestSub(subParams);

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        BatchUnsubReply.Result result = requestUnsub(unsubParams).get(0);
        assertEquals(result.getCode(), BatchUnsubReply.Code.OK);
    }

    @Test(groups = "integration")
    public void subExceedLimit() {
        long now = 0;
        String tenantId = "tenantId";
        String inboxId = "inboxId-" + System.nanoTime();
        String topicFilter = "/a/b/c";
        String topicFilter1 = "/a";
        long incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        BatchAttachRequest.Params attachParams = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setNow(now)
            .build();
        InboxVersion inboxVersion = requestAttach(attachParams).get(0);

        BatchSubRequest.Params subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setMaxTopicFilters(1)
            .setNow(now)
            .build();
        requestSub(subParams);

        subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter1)
            .setMaxTopicFilters(1)
            .setNow(now)
            .build();
        BatchSubReply.Code code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.EXCEED_LIMIT);

        BatchUnsubRequest.Params unsubParams = BatchUnsubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter)
            .setNow(now)
            .build();
        requestUnsub(unsubParams);

        // sub again
        subParams = BatchSubRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(inboxVersion)
            .setTopicFilter(topicFilter1)
            .setMaxTopicFilters(100)
            .setNow(now)
            .build();
        code = requestSub(subParams).get(0);
        assertEquals(code, BatchSubReply.Code.OK);
    }
}
