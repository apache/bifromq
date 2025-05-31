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

package org.apache.bifromq.inbox.store.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class KVSchemaUtilTest {
    @Test
    public void buildAndParse() {
        String tenantId = "tenantA";
        String inboxId = "inboxA";
        long incarnation = System.nanoTime();
        ByteString tenantPrefix = KVSchemaUtil.tenantBeginKeyPrefix(tenantId);
        ByteString inboxPrefix = KVSchemaUtil.inboxStartKeyPrefix(tenantId, inboxId);
        ByteString inboxBucketPrefix = KVSchemaUtil.inboxBucketStartKeyPrefix(tenantId, inboxId);
        ByteString inboxKeyPrefix = KVSchemaUtil.inboxInstanceStartKey(tenantId, inboxId, incarnation);
        ByteString qos0MsgKey = KVSchemaUtil.qos0MsgKey(inboxKeyPrefix, 1);
        ByteString bufferMsgKey = KVSchemaUtil.bufferedMsgKey(inboxKeyPrefix, 1);

        assertFalse(KVSchemaUtil.isInboxInstanceKey(tenantPrefix));
        assertFalse(KVSchemaUtil.isInboxInstanceKey(inboxPrefix));
        assertTrue(KVSchemaUtil.isInboxInstanceKey(inboxKeyPrefix));
        assertTrue(KVSchemaUtil.isInboxInstanceKey(qos0MsgKey));
        assertTrue(KVSchemaUtil.isInboxInstanceKey(bufferMsgKey));

        assertTrue(inboxPrefix.startsWith(tenantPrefix));
        assertTrue(inboxKeyPrefix.startsWith(inboxPrefix));

        assertEquals(KVSchemaUtil.parseTenantId(tenantPrefix), tenantId);
        assertEquals(KVSchemaUtil.parseTenantId(inboxPrefix), tenantId);
        assertEquals(KVSchemaUtil.parseTenantId(inboxKeyPrefix), tenantId);

        assertEquals(KVSchemaUtil.parseInboxBucketPrefix(inboxPrefix), inboxBucketPrefix);
        assertEquals(KVSchemaUtil.parseInboxBucketPrefix(inboxKeyPrefix), inboxBucketPrefix);

        assertTrue(KVSchemaUtil.isInboxInstanceStartKey(inboxKeyPrefix));

        assertEquals(KVSchemaUtil.parseSeq(inboxKeyPrefix, qos0MsgKey), 1);
        assertEquals(KVSchemaUtil.parseTenantId(qos0MsgKey), tenantId);
        assertEquals(KVSchemaUtil.parseInboxInstanceStartKeyPrefix(qos0MsgKey), inboxKeyPrefix);

        assertEquals(KVSchemaUtil.parseSeq(inboxKeyPrefix, bufferMsgKey), 1);
        assertEquals(KVSchemaUtil.parseTenantId(bufferMsgKey), tenantId);
        assertEquals(KVSchemaUtil.parseInboxInstanceStartKeyPrefix(bufferMsgKey), inboxKeyPrefix);
    }
}
