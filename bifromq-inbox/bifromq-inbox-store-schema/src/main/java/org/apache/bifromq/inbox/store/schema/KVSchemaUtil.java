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

package org.apache.bifromq.inbox.store.schema;

import static org.apache.bifromq.util.BSUtil.toByteString;
import static org.apache.bifromq.util.BSUtil.toShort;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;

/**
 * Utility for working with the data stored in inbox store.
 */
public class KVSchemaUtil {
    // the first byte indicating the version of KV schema for storing inbox data
    public static final ByteString SCHEMA_VER = ByteString.copyFrom(new byte[] {0x01});
    private static final ByteString QOS0_QUEUE_SIGN = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString SEND_BUFFER_SIGN = ByteString.copyFrom(new byte[] {0x01});

    private static final int MAX_BUCKETS = 0xFF; // 256

    public static ByteString tenantBeginKeyPrefix(String tenantId) {
        // tenantBeginKeyPrefix: <SCHEMA_VER><tenantIdLength><tenantId>
        ByteString tenantIdBS = unsafeWrap(tenantId.getBytes(StandardCharsets.UTF_8));
        return SCHEMA_VER.concat(toByteString((short) tenantIdBS.size())).concat(tenantIdBS);
    }

    private static short tenantIdLength(ByteString key) {
        return toShort(key.substring(SCHEMA_VER.size(), SCHEMA_VER.size() + Short.BYTES));
    }

    private static int tenantBeginKeyPrefixLength(ByteString key) {
        // tenantBeginKeyPrefix: <SCHEMA_VER><tenantIdLength><tenantId>
        return SCHEMA_VER.size() + Short.BYTES + tenantIdLength(key);
    }

    private static int inboxBucketStartKeyPrefixLength(ByteString key) {
        // inboxBucketStartKeyPrefix: <TENANT_BEGIN_KEY_PREFIX><BUCKET_BYTE>
        int tenantPrefixLength = tenantBeginKeyPrefixLength(key);
        return tenantPrefixLength + 1;
    }

    public static ByteString inboxBucketStartKeyPrefix(String tenantId, String inboxId) {
        // inboxBucketStartKeyPrefix: <TENANT_BEGIN_KEY_PREFIX><BUCKET_BYTE>
        return inboxBucketStartKeyPrefix(tenantId, bucket(inboxId));
    }

    public static ByteString inboxBucketStartKeyPrefix(String tenantId, byte bucket) {
        // inboxBucketStartKeyPrefix: <TENANT_BEGIN_KEY_PREFIX><BUCKET_BYTE>
        return tenantBeginKeyPrefix(tenantId).concat(unsafeWrap(new byte[] {bucket}));
    }

    public static ByteString inboxStartKeyPrefix(String tenantId, String inboxId) {
        // inboxStartKeyPrefix: <TENANT_BEGIN_KEY_PREFIX><BUCKET_BYTE><inboxIdLength><inboxId>
        ByteString inboxIdBS = unsafeWrap(inboxId.getBytes(StandardCharsets.UTF_8));
        return inboxBucketStartKeyPrefix(tenantId, inboxId)
            .concat(toByteString((short) inboxIdBS.size()))
            .concat(inboxIdBS);
    }

    private static byte bucket(String inboxId) {
        int hash = inboxId.hashCode();
        return (byte) ((hash ^ (hash >>> 16)) & MAX_BUCKETS);
    }

    private static short inboxIdLength(ByteString key, int tenantPrefixLength) {
        return toShort(key.substring(tenantPrefixLength + 1, tenantPrefixLength + 1 + Short.BYTES));
    }

    private static int inboxStartKeyPrefixLength(ByteString inboxStoreKey) {
        // inboxStartKeyPrefix: <TENANT_BEGIN_KEY_PREFIX><BUCKET_BYTE><inboxIdLength><inboxId>
        int tenantPrefixLength = tenantBeginKeyPrefixLength(inboxStoreKey);
        short inboxIdLen = inboxIdLength(inboxStoreKey, tenantPrefixLength);
        return tenantPrefixLength + 1 + Short.BYTES + inboxIdLen;
    }

    public static ByteString inboxInstanceStartKey(String tenantId, String inboxId, long incarnation) {
        // inboxInstanceStartKeyPrefix: <INBOX_START_KEY_PREFIX><incarnation>
        return inboxStartKeyPrefix(tenantId, inboxId).concat(toByteString(incarnation));
    }

    private static int inboxInstanceStartKeyPrefixLength(ByteString inboxStoreKey) {
        // inboxInstanceStartKeyPrefix: <INBOX_INSTANCE_START_KEY_PREFIX><incarnation>
        return inboxStartKeyPrefixLength(inboxStoreKey) + Long.BYTES;
    }

    public static boolean isInboxInstanceStartKey(ByteString inboxStoreKey) {
        return inboxStoreKey.size() == inboxInstanceStartKeyPrefixLength(inboxStoreKey);
    }

    public static boolean isInboxInstanceKey(ByteString inboxStoreKey) {
        if (inboxStoreKey.size() > SCHEMA_VER.size() + Short.BYTES) {
            int tenantBeginKeyPrefixLength = tenantBeginKeyPrefixLength(inboxStoreKey);
            if (inboxStoreKey.size() > tenantBeginKeyPrefixLength + 1 + Short.BYTES) {
                int inboxStartKeyPrefixLength = inboxStartKeyPrefixLength(inboxStoreKey);
                return inboxStoreKey.size() >= inboxStartKeyPrefixLength + Long.BYTES;
            }
        }
        return false;
    }

    public static String parseTenantId(ByteString inboxStoreKey) {
        int tenantIdLength = tenantIdLength(inboxStoreKey);
        int startAt = SCHEMA_VER.size() + Short.BYTES;
        return inboxStoreKey.substring(startAt, startAt + tenantIdLength).toStringUtf8();
    }

    public static ByteString parseInboxBucketPrefix(ByteString inboxKey) {
        return inboxKey.substring(0, inboxBucketStartKeyPrefixLength(inboxKey));
    }

    public static ByteString parseInboxInstanceStartKeyPrefix(ByteString inboxKey) {
        return inboxKey.substring(0, inboxInstanceStartKeyPrefixLength(inboxKey));
    }

    public static ByteString qos0QueuePrefix(ByteString inboxInstanceStartKey) {
        return inboxInstanceStartKey.concat(QOS0_QUEUE_SIGN);
    }

    public static ByteString qos0MsgKey(ByteString inboxInstanceStartKey, long seq) {
        return qos0QueuePrefix(inboxInstanceStartKey).concat(toByteString(seq));
    }

    public static ByteString sendBufferPrefix(ByteString inboxInstanceStartKey) {
        return inboxInstanceStartKey.concat(SEND_BUFFER_SIGN);
    }

    public static ByteString bufferedMsgKey(ByteString inboxInstanceStartKey, long seq) {
        return sendBufferPrefix(inboxInstanceStartKey).concat(toByteString(seq));
    }

    public static long parseSeq(ByteString inboxInstanceStartKey, ByteString inboxMsgKey) {
        // QOS0 MessageKey: <INBOX_INSTANCE_START_KEY_PREFIX><QOS0_QUEUE_SIGN><SEQ>
        // QOS1 MessageKey: <INBOX_INSTANCE_START_KEY_PREFIX><SEND_BUFFER_SIGN_SIGN><SEQ>
        return inboxMsgKey.substring(inboxInstanceStartKey.size() + 1).asReadOnlyByteBuffer().getLong();
    }
}
