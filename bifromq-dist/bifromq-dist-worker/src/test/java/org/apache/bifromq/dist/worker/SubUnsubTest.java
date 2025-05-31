/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.dist.worker;

import static org.testng.Assert.assertEquals;

import org.apache.bifromq.dist.rpc.proto.BatchMatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchReply;
import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.util.TopicUtil;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class SubUnsubTest extends DistWorkerTest {
    @Test(groups = "integration")
    public void normalSub() {
        String topicFilter = "/a/b/c";
        BatchMatchReply.TenantBatch.Code result = match("tenantA", topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);

        result = match("tenantA", topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test(groups = "integration")
    public void duplicateSub() {
        String topicFilter = "/a/b/c";
        MatchRoute route1 = MatchRoute.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setBrokerId(MqttBroker)
            .setReceiverId("inbox1")
            .setDelivererKey("server1")
            .setIncarnation(1L)
            .build();

        MatchRoute route2 = MatchRoute.newBuilder()
            .setMatcher(TopicUtil.from(topicFilter))
            .setBrokerId(MqttBroker)
            .setReceiverId("inbox1")
            .setDelivererKey("server1")
            .setIncarnation(2L)
            .build();

        List<BatchMatchReply.TenantBatch.Code> results = match("tenantA", 10, route1, route2);
        assertEquals(results.get(0), BatchMatchReply.TenantBatch.Code.OK);
        assertEquals(results.get(1), BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test(groups = "integration")
    public void sharedSub() {
        String topicFilter = "$share/group/a/b/c";

        BatchMatchReply.TenantBatch.Code result = match(tenantA, topicFilter, MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);

        result = match(tenantA, topicFilter, MqttBroker, "inbox1", "server2");
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test(groups = "integration")
    public void normalUnsub() {
        BatchUnmatchReply.TenantBatch.Code result = unmatch(tenantB, "/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED);
        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
        result = unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.TenantBatch.Code.OK);

        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "server2");
        result = unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "server2");
        assertEquals(result, BatchUnmatchReply.TenantBatch.Code.OK);
    }

    @Test(groups = "integration")
    public void sharedUnsub() {
        BatchUnmatchReply.TenantBatch.Code result =
            unmatch(tenantB, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED);
        match(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");
        result = unmatch(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server1");
        assertEquals(result, BatchUnmatchReply.TenantBatch.Code.OK);

        match(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server2");
        result = unmatch(tenantA, "$share/group/a/b/c", MqttBroker, "inbox1", "server2");
        assertEquals(result, BatchUnmatchReply.TenantBatch.Code.OK);
    }

    @Test(groups = "integration")
    public void sharedSubExceedLimit() {
        BatchMatchReply.TenantBatch.Code result =
            match(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox1", "server1", 2);
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);
        result = match(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox2", "server1", 2);
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);

        result = match(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox3", "server1", 2);
        assertEquals(result, BatchMatchReply.TenantBatch.Code.EXCEED_LIMIT);

        unmatch(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox2", "server1");

        result = match(tenantA, "$share/sharedSubExceedLimit/a/b/c", MqttBroker, "inbox3", "server1");
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);
    }

    @Test(groups = "integration")
    public void normalResubWithIncarUpdated() {
        String topicFilter = "/a/b/c";

        BatchMatchReply.TenantBatch.Code result = match(tenantA, topicFilter, MqttBroker, "inbox1", "server1", 1L);
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);

        result = match(tenantA, topicFilter, MqttBroker, "inbox1", "server1", 2L);
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);

        BatchUnmatchReply.TenantBatch.Code unmatchResult =
            unmatch(tenantA, topicFilter, MqttBroker, "inbox1", "server1", 1L);
        assertEquals(unmatchResult, BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED);

        unmatchResult = unmatch(tenantA, topicFilter, MqttBroker, "inbox1", "server1", 2L);
        assertEquals(unmatchResult, BatchUnmatchReply.TenantBatch.Code.OK);
    }

    @Test(groups = "integration")
    public void sharedResubWithIncarUpdate() {
        String topicFilter = "$share/group/a/b/c";

        BatchMatchReply.TenantBatch.Code result = match(tenantA, topicFilter, MqttBroker, "inbox1", "server1", 1L);
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);

        result = match(tenantA, topicFilter, MqttBroker, "inbox1", "server1", 2L);
        assertEquals(result, BatchMatchReply.TenantBatch.Code.OK);

        BatchUnmatchReply.TenantBatch.Code unmatchResult =
            unmatch(tenantA, topicFilter, MqttBroker, "inbox1", "server1", 1L);
        assertEquals(unmatchResult, BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED);

        unmatchResult = unmatch(tenantA, topicFilter, MqttBroker, "inbox1", "server1", 2L);
        assertEquals(unmatchResult, BatchUnmatchReply.TenantBatch.Code.OK);
    }
}
