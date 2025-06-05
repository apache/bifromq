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

package org.apache.bifromq.dist.worker.schema;

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.util.TopicUtil;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class GroupMatchingTest {

    @Test
    public void groupMatch() {
        matchInfo("$share/group1/home/sensor/temperature");
        matchInfo("$oshare/group1/home/sensor/temperature");
    }

    @Test
    public void equality() {
        String tenantId = "tenant1";
        ByteString key1 =
            KVSchemaUtil.toGroupRouteKey(tenantId, TopicUtil.from("$share/group1/home/sensor/temperature"));
        ByteString key2 =
            KVSchemaUtil.toGroupRouteKey(tenantId, TopicUtil.from("$share/group2/home/sensor/temperature"));
        RouteGroup groupMembers = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox1", "deliverer1"), 1)
            .build();
        GroupMatching matching1 = (GroupMatching) KVSchemaUtil.buildMatchRoute(key1, groupMembers.toByteString());
        GroupMatching matching2 = (GroupMatching) KVSchemaUtil.buildMatchRoute(key2, groupMembers.toByteString());
        assertNotEquals(matching1, matching2);
    }

    private void matchInfo(String topicFilter) {
        String tenantId = "tenant1";

        ByteString key = KVSchemaUtil.toGroupRouteKey(tenantId, TopicUtil.from(topicFilter));
        RouteGroup groupMembers = RouteGroup.newBuilder()
            .putMembers(toReceiverUrl(1, "inbox1", "deliverer1"), 1)
            .build();

        GroupMatching matching = (GroupMatching) KVSchemaUtil.buildMatchRoute(key, groupMembers.toByteString());

        for (NormalMatching normalMatching : matching.receiverList) {
            assertEquals(normalMatching.matchInfo().getMatcher().getMqttTopicFilter(), topicFilter);
        }
    }
}