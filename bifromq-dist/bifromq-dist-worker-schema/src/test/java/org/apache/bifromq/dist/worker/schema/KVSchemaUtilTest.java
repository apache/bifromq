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

package org.apache.bifromq.dist.worker.schema;

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.buildMatchRoute;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.BSUtil;
import org.apache.bifromq.util.TopicUtil;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class KVSchemaUtilTest {
    private static final int MqttBroker = 0;
    private static final int InboxService = 1;
    private static final int RuleEngine = 2;

    @Test
    public void testParseNormalMatchRecord() {
        String tenantId = "tenantId";
        String topicFilter = "/a/b/c";
        RouteMatcher matcher = TopicUtil.from(topicFilter);
        String receiverUrl = toReceiverUrl(MqttBroker, "inbox1", "delivererKey1");
        MatchRoute route = MatchRoute.newBuilder()
            .setMatcher(matcher)
            .setBrokerId(MqttBroker)
            .setReceiverId("inbox1")
            .setDelivererKey("delivererKey1")
            .setIncarnation(1L)
            .build();
        ByteString key = toNormalRouteKey(tenantId, matcher, toReceiverUrl(route));
        Matching matching = buildMatchRoute(key, BSUtil.toByteString(route.getIncarnation()));
        assertEquals(matching.tenantId(), tenantId);
        assertEquals(matching.mqttTopicFilter(), topicFilter);
        assertTrue(matching instanceof NormalMatching);
        assertEquals(((NormalMatching) matching).receiverUrl(), receiverUrl);

        MatchInfo matchInfo = ((NormalMatching) matching).matchInfo();
        assertEquals(matchInfo.getReceiverId(), route.getReceiverId());
        assertEquals(matchInfo.getIncarnation(), route.getIncarnation());

        assertEquals(((NormalMatching) matching).subBrokerId(), MqttBroker);
        assertEquals(((NormalMatching) matching).delivererKey(), route.getDelivererKey());
    }

    @Test
    public void testParseGroupMatchRecord() {
        String origTopicFilter = "$share/group//a/b/c";
        RouteMatcher matcher = TopicUtil.from(origTopicFilter);
        MatchRoute route = MatchRoute.newBuilder()
            .setMatcher(matcher)
            .setBrokerId(MqttBroker)
            .setReceiverId("inbox1")
            .setDelivererKey("server1")
            .setIncarnation(1L)
            .build();

        String scopedReceiverId = toReceiverUrl(MqttBroker, "inbox1", "server1");
        ByteString key = toGroupRouteKey("tenantId", matcher);
        RouteGroup groupMembers = RouteGroup.newBuilder()
            .putMembers(scopedReceiverId, route.getIncarnation())
            .build();
        Matching matching = buildMatchRoute(key, groupMembers.toByteString());
        assertEquals(matching.tenantId(), "tenantId");
        assertEquals(matching.matcher.getFilterLevelList(), TopicUtil.parse("/a/b/c", false));
        assertEquals(matching.mqttTopicFilter(), origTopicFilter);
        assertTrue(matching instanceof GroupMatching);
        assertEquals(((GroupMatching) matching).receivers().get(scopedReceiverId), 1);
        assertEquals(((GroupMatching) matching).receiverList.get(0).receiverUrl(), scopedReceiverId);
        assertEquals(((GroupMatching) matching).receiverList.get(0).incarnation(), route.getIncarnation());

        MatchInfo matchInfo = ((GroupMatching) matching).receiverList.get(0).matchInfo();
        assertEquals(matchInfo.getReceiverId(), "inbox1");
        assertEquals(((GroupMatching) matching).receiverList.get(0).subBrokerId(), MqttBroker);
        assertEquals(((GroupMatching) matching).receiverList.get(0).delivererKey(), "server1");
    }
}
