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

package org.apache.bifromq.dist.worker.schema;

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.apache.bifromq.util.BSUtil.toByteString;
import static org.testng.Assert.assertNotEquals;

import org.apache.bifromq.util.TopicUtil;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class NormalMatchingTest {

    @Test
    public void equality() {
        String tenantId = "tenant1";
        ByteString key1 = toNormalRouteKey(tenantId, TopicUtil.from("#"), toReceiverUrl(1, "receiver1", "deliverer1"));
        ByteString key2 = toNormalRouteKey(tenantId, TopicUtil.from("+"), toReceiverUrl(1, "receiver1", "deliverer1"));
        NormalMatching matching1 = (NormalMatching) KVSchemaUtil.buildMatchRoute(key1, toByteString(1L));
        NormalMatching matching2 = (NormalMatching) KVSchemaUtil.buildMatchRoute(key2, toByteString(1L));
        assertNotEquals(matching1, matching2);
    }
}