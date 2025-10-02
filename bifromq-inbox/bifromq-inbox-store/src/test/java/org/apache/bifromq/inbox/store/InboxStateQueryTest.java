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

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.KVRangeRWReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDetachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchFetchInboxStateReply;
import org.apache.bifromq.inbox.storage.proto.BatchFetchInboxStateRequest;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceROCoProcOutput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcInput;
import org.apache.bifromq.inbox.storage.proto.InboxServiceRWCoProcOutput;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class InboxStateQueryTest extends InboxStoreTest {
    @Test(groups = "integration")
    public void fetchStateNoInbox() {
        String tenantId = "tenant1";
        String inboxId = "user1/client1" + System.nanoTime();
        long now = 1000L;

        BatchFetchInboxStateReply reply = requestFetchInboxState(tenantId, inboxId, now);
        assertEquals(reply.getResultCount(), 1);
        assertEquals(reply.getResult(0).getCode(), BatchFetchInboxStateReply.Result.Code.NO_INBOX);
    }

    @Test(groups = "integration")
    public void fetchStateOK() {
        String tenantId = "tenant1";
        String inboxId = "user1/client1" + System.nanoTime();
        long now = 1000L;

        // attach inbox
        attachInbox(tenantId, inboxId, 1, 60, 100, false, now);

        BatchFetchInboxStateReply reply = requestFetchInboxState(tenantId, inboxId, now);
        assertEquals(reply.getResultCount(), 1);
        assertEquals(reply.getResult(0).getCode(), BatchFetchInboxStateReply.Result.Code.OK);
        assertTrue(reply.getResult(0).hasState());
        assertEquals(reply.getResult(0).getState().getExpirySeconds(), 60);
        assertEquals(reply.getResult(0).getState().getLimit(), 100);
        assertFalse(reply.getResult(0).getState().getDropOldest());
    }

    @Test(groups = "integration")
    public void fetchStateExpiredAfterDetach() {
        String tenantId = "tenant1";
        String inboxId = "user1/client1" + System.nanoTime();
        long attachNow = 1000L;

        // attach then detach
        InboxVersion version = attachInbox(tenantId, inboxId, 1, 1, 10, false, attachNow);
        long detachedNow = 2000L;
        detachInbox(tenantId, inboxId, version, 1, detachedNow, true);

        long queryNow = 3005L; // detachedNow + expirySeconds*1000 + 5
        BatchFetchInboxStateReply reply = requestFetchInboxState(tenantId, inboxId, queryNow);
        assertEquals(reply.getResultCount(), 1);
        assertEquals(reply.getResult(0).getCode(), BatchFetchInboxStateReply.Result.Code.EXPIRED);
    }

    private BatchFetchInboxStateReply requestFetchInboxState(String tenantId, String inboxId, long now) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(tenantId, inboxId);
        BatchFetchInboxStateRequest request = BatchFetchInboxStateRequest.newBuilder()
            .addParams(BatchFetchInboxStateRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setNow(now)
                .build())
            .build();
        InboxServiceROCoProcInput input = InboxServiceROCoProcInput.newBuilder()
            .setReqId(reqId)
            .setFetchInboxState(request)
            .build();
        KVRangeSetting s = findByKey(routeKey, storeClient.latestEffectiveRouter()).get();
        KVRangeROReply roReply = storeClient.query(s.leader(), KVRangeRORequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver())
            .setKvRangeId(s.id())
            .setRoCoProc(ROCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(roReply.getCode(), ReplyCode.Ok);
        InboxServiceROCoProcOutput output = roReply.getRoCoProcResult().getInboxService();
        assertEquals(output.getReqId(), reqId);
        return output.getFetchInboxState();
    }

    private org.apache.bifromq.inbox.storage.proto.InboxVersion attachInbox(String tenantId,
                                                                            String inboxId,
                                                                            long incarnation,
                                                                            int expirySeconds,
                                                                            int limit,
                                                                            boolean dropOldest,
                                                                            long now) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(tenantId, inboxId);
        BatchAttachRequest request = BatchAttachRequest.newBuilder()
            .addParams(BatchAttachRequest.Params.newBuilder()
                .setInboxId(inboxId)
                .setIncarnation(incarnation)
                .setExpirySeconds(expirySeconds)
                .setLimit(limit)
                .setDropOldest(dropOldest)
                .setClient(ClientInfo.newBuilder().setTenantId(tenantId).build())
                .setNow(now)
                .build())
            .build();
        InboxServiceRWCoProcInput input = MessageUtil.buildAttachRequest(reqId, request);
        KVRangeSetting s = findByKey(routeKey, storeClient.latestEffectiveRouter()).get();
        KVRangeRWReply rwReply = storeClient.execute(s.leader(), KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver())
            .setKvRangeId(s.id())
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(rwReply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = rwReply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchAttach());
        return output.getBatchAttach().getVersion(0);
    }

    private void detachInbox(String tenantId,
                             String inboxId,
                             org.apache.bifromq.inbox.storage.proto.InboxVersion version,
                             int expirySeconds,
                             long now,
                             boolean discardLWT) {
        long reqId = ThreadLocalRandom.current().nextInt();
        ByteString routeKey = inboxStartKeyPrefix(tenantId, inboxId);
        BatchDetachRequest request = BatchDetachRequest.newBuilder()
            .addParams(BatchDetachRequest.Params.newBuilder()
                .setTenantId(tenantId)
                .setInboxId(inboxId)
                .setVersion(version)
                .setExpirySeconds(expirySeconds)
                .setDiscardLWT(discardLWT)
                .setNow(now)
                .build())
            .build();
        InboxServiceRWCoProcInput input = MessageUtil.buildDetachRequest(reqId, request);
        KVRangeSetting s = findByKey(routeKey, storeClient.latestEffectiveRouter()).get();
        KVRangeRWReply rwReply = storeClient.execute(s.leader(), KVRangeRWRequest.newBuilder()
            .setReqId(reqId)
            .setVer(s.ver())
            .setKvRangeId(s.id())
            .setRwCoProc(RWCoProcInput.newBuilder().setInboxService(input).build())
            .build()).join();
        assertEquals(rwReply.getCode(), ReplyCode.Ok);
        InboxServiceRWCoProcOutput output = rwReply.getRwCoProcResult().getInboxService();
        assertTrue(output.hasBatchDetach());
        assertEquals(output.getBatchDetach().getCodeList(), List.of(
            org.apache.bifromq.inbox.storage.proto.BatchDetachReply.Code.OK));
    }
}

