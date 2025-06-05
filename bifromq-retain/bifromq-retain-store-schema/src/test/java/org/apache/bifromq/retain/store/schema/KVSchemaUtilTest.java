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

import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.filterPrefix;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.parseTenantId;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.retainKeyPrefix;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.retainMessageKey;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.util.BSUtil.toByteString;
import static org.apache.bifromq.util.TopicUtil.isMultiWildcardTopicFilter;
import static org.apache.bifromq.util.TopicUtil.parse;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.List;
import org.testng.annotations.Test;

public class KVSchemaUtilTest {

    private static ByteString levelByte(int value) {
        return toByteString((short) value);
    }

    @Test
    public void testRetainMessageKeyPrefix() {
        String tenantId = "tenantA";
        ByteString tenantNS = tenantBeginKey(tenantId);
        assertRetainMessageKeyPrefix(tenantId, "#", tenantNS.concat(levelByte(0)));
        assertRetainMessageKeyPrefix(tenantId, "/#", tenantNS.concat(levelByte(1)).concat(LevelHash.hash(List.of(""))));
        assertRetainMessageKeyPrefix(tenantId, "+", tenantNS.concat(levelByte(1)));
        assertRetainMessageKeyPrefix(tenantId, "+/#", tenantNS.concat(levelByte(1)));
        assertRetainMessageKeyPrefix(tenantId, "a/#",
            tenantNS.concat(levelByte(1).concat(LevelHash.hash(List.of("a")))));
        assertRetainMessageKeyPrefix(tenantId, "/a",
            tenantNS.concat(levelByte(2).concat(LevelHash.hash(List.of("", "a")))));
        assertRetainMessageKeyPrefix(tenantId, "a/+",
            tenantNS.concat(levelByte(2).concat(LevelHash.hash(List.of("a")))));

        assertRetainMessageKeyPrefix(tenantId, "a/b",
            tenantNS.concat(levelByte(2).concat(LevelHash.hash(List.of("a", "b")))));

        assertRetainMessageKeyPrefix(tenantId, "/a/#",
            tenantNS.concat(levelByte(2).concat(LevelHash.hash(List.of("", "a")))));

        assertRetainMessageKeyPrefix(tenantId, "/a/+", tenantNS.concat(
            levelByte(3).concat(LevelHash.hash(List.of("", "a")))));

        assertRetainMessageKeyPrefix(tenantId, "/a/+/+", tenantNS.concat(
            levelByte(4).concat(LevelHash.hash(List.of("", "a")))));

        assertRetainMessageKeyPrefix(tenantId, "/+/b/",
            tenantNS.concat(levelByte(4).concat(LevelHash.hash(List.of("")))));
        assertRetainMessageKeyPrefix(tenantId, "/+/b/+/",
            tenantNS.concat(levelByte(5).concat(LevelHash.hash(List.of("")))));
    }

    private void assertRetainMessageKeyPrefix(String tenantId, String topicFilter, ByteString bytes) {
        assertEquals(toRetainMessageKeyPrefix(tenantId, topicFilter), bytes);
    }

    private ByteString toRetainMessageKeyPrefix(String tenantId, String topicFilter) {
        List<String> filterLevels = parse(topicFilter, false);
        List<String> filterPrefix = filterPrefix(filterLevels);
        short levels =
            (short) (isMultiWildcardTopicFilter(topicFilter) ? filterLevels.size() - 1 : filterLevels.size());
        return retainKeyPrefix(tenantId, levels, filterPrefix);
    }

    @Test
    public void testParseTenantBeginKey() {
        String tenantId = "tenantA";
        ByteString tenantNS = tenantBeginKey(tenantId);
        assertEquals(parseTenantId(tenantNS), tenantId);

        assertEquals(parseTenantId(retainMessageKey(tenantId, "/a/b/c")), tenantId);
        assertEquals(parseTenantId(toRetainMessageKeyPrefix(tenantId, "/a/b/c")), tenantId);
    }

    @Test
    public void testFilterPrefix() {
        List<String> filterLevels = parse("/a/b/+", false);
        assertEquals(filterPrefix(filterLevels), filterLevels.subList(0, 3));

        filterLevels = parse("/a/b/c", false);
        assertEquals(filterPrefix(filterLevels), filterLevels);

        filterLevels = parse("/", false);
        assertEquals(filterPrefix(filterLevels), filterLevels);

        filterLevels = parse("/#", false);
        assertEquals(filterPrefix(filterLevels), filterLevels.subList(0, filterLevels.size() - 1));

        filterLevels = parse("#", false);
        assertTrue(filterPrefix(filterLevels).isEmpty());

        filterLevels = parse("+", false);
        assertTrue(filterPrefix(filterLevels).isEmpty());

        filterLevels = parse("+/#", false);
        assertTrue(filterPrefix(filterLevels).isEmpty());
    }
}
