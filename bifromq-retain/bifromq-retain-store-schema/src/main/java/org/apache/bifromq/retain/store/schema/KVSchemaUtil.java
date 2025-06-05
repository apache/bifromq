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

package org.apache.bifromq.retain.store.schema;

import static org.apache.bifromq.util.BSUtil.toByteString;
import static org.apache.bifromq.util.BSUtil.toShort;
import static org.apache.bifromq.util.TopicConst.MULTI_WILDCARD;
import static org.apache.bifromq.util.TopicConst.SINGLE_WILDCARD;
import static org.apache.bifromq.util.TopicUtil.escape;
import static org.apache.bifromq.util.TopicUtil.parse;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Utility for working with the data stored in retain store.
 */
public class KVSchemaUtil {
    // the first byte indicating the version of KV schema for storing inbox data
    public static final ByteString SCHEMA_VER = ByteString.copyFrom(new byte[] {0x00});

    private static final int TENANT_ID_PREFIX_LENGTH = SCHEMA_VER.size() + Short.BYTES;

    public static ByteString tenantBeginKey(String tenantId) {
        ByteString tenantIdBS = unsafeWrap(tenantId.getBytes(StandardCharsets.UTF_8));
        return SCHEMA_VER.concat(toByteString((short) tenantIdBS.size())).concat(tenantIdBS);
    }

    public static ByteString retainMessageKey(String tenantId, String topic) {
        List<String> topicLevels = parse(topic, false);
        short levels = (short) topicLevels.size();
        return tenantBeginKey(tenantId).concat(toByteString(levels))
            .concat(LevelHash.hash(topicLevels))
            .concat(copyFromUtf8(escape(topic)));
    }

    public static List<String> filterPrefix(List<String> filterLevels) {
        int firstWildcard = filterLevels.indexOf(SINGLE_WILDCARD);
        if (firstWildcard == -1) {
            if (filterLevels.get(filterLevels.size() - 1).equals(MULTI_WILDCARD)) {
                firstWildcard = filterLevels.size() - 1;
                return filterLevels.subList(0, firstWildcard);
            }
            return filterLevels;
        }
        return filterLevels.subList(0, firstWildcard);
    }

    public static ByteString retainKeyPrefix(String tenantId, short levels, List<String> filterPrefix) {
        return tenantBeginKey(tenantId)
            .concat(toByteString(levels))
            .concat(LevelHash.hash(filterPrefix));
    }

    public static String parseTenantId(ByteString key) {
        return key.substring(TENANT_ID_PREFIX_LENGTH, TENANT_ID_PREFIX_LENGTH + tenantIdLength(key)).toStringUtf8();
    }

    public static ByteString parseLevelHash(ByteString key) {
        short tenantIdLength = tenantIdLength(key);
        int levelBytesIdx = TENANT_ID_PREFIX_LENGTH + tenantIdLength;
        int levelHashIdx = TENANT_ID_PREFIX_LENGTH + tenantIdLength + Short.BYTES;
        short levels = toShort(key.substring(levelBytesIdx, levelHashIdx));
        return key.substring(levelHashIdx, levelHashIdx + levels);
    }

    private static short tenantIdLength(ByteString key) {
        return toShort(key.substring(SCHEMA_VER.size(), TENANT_ID_PREFIX_LENGTH));
    }
}
