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
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxInstanceStartKey;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.InboxMetadata;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.type.ClientInfo;
import org.testng.annotations.Test;

public class InboxMetadataCreatedAtTest extends InboxStoreTest {

    private InboxMetadata readMetadata(String tenantId, String inboxId, long incarnation) throws Exception {
        ByteString key = inboxInstanceStartKey(tenantId, inboxId, incarnation);
        KVRangeSetting s = findByKey(key, storeClient.latestEffectiveRouter()).get();
        long reqId = System.nanoTime();
        KVRangeROReply reply = storeClient
            .query(s.leader(), KVRangeRORequest.newBuilder()
                .setReqId(reqId)
                .setVer(s.ver())
                .setKvRangeId(s.id())
                .setGetKey(key)
                .build())
            .join();
        assertEquals(reply.getReqId(), reqId);
        assertEquals(reply.getCode(), ReplyCode.Ok);
        ByteString value = reply.getGetResult().getValue();
        return InboxMetadata.parseFrom(value);
    }

    @Test(groups = "integration")
    public void createdAtOnNewAttach() throws Exception {
        long now = HLC.INST.getPhysical();
        String tenantId = "tenant-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();

        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        InboxVersion ver = requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(10)
            .setLimit(100)
            .setClient(client)
            .setNow(now)
            .build()).get(0);

        InboxMetadata md = readMetadata(tenantId, inboxId, incarnation);
        assertEquals(md.getCreatedAt(), now);
        assertEquals(md.getMod(), ver.getMod());
        assertEquals(md.getIncarnation(), ver.getIncarnation());
    }

    @Test(groups = "integration")
    public void createdAtNotChangedOnReattachExisting() throws Exception {
        long t1 = HLC.INST.getPhysical();
        String tenantId = "tenant-" + System.nanoTime();
        String inboxId = "inbox-" + System.nanoTime();
        long incarnation = System.nanoTime();

        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();
        requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(30)
            .setLimit(100)
            .setClient(client)
            .setNow(t1)
            .build()).get(0);

        InboxMetadata md1 = readMetadata(tenantId, inboxId, incarnation);

        long t2 = t1 + 1000;
        // reattach same incarnation with newer now
        requestAttach(BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(30)
            .setLimit(100)
            .setClient(client)
            .setNow(t2)
            .build()).get(0);

        InboxMetadata md2 = readMetadata(tenantId, inboxId, incarnation);
        // createdAt should be preserved
        assertEquals(md2.getCreatedAt(), md1.getCreatedAt());
        assertEquals(md1.getCreatedAt(), t1);
    }
}
