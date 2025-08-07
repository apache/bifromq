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

package org.apache.bifromq.dist.worker;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.apache.bifromq.type.QoS.AT_MOST_ONCE;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.dist.rpc.proto.BatchDistReply;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryPackage;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.TopicMessagePack;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class DistQoS0Test extends DistWorkerTest {

    @BeforeMethod(groups = "integration")
    public void mock() {
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(receiverManager.get(InboxService)).thenReturn(inboxBroker);

        when(writer1.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
        when(writer2.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
        when(writer3.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
    }

    @AfterMethod(groups = "integration")
    public void clearMock() {
        Mockito.reset(writer1, writer2, writer3);
    }

    @Test(groups = "integration")
    public void succeedWithNoSub() {
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, topic, payload, "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().getOrDefault(topic, 0).intValue(), 0);
    }

    @Test(groups = "integration")
    public void testDistCase1() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        match(tenantA, "TopicA/#", MqttBroker, "inbox1", "batch1");

        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "TopicB", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().getOrDefault("TopicB", 0).intValue(), 0);

        unmatch(tenantA, "TopicA/#", MqttBroker, "inbox1", "batch1");
    }

    @Test(groups = "integration")
    public void testDistCase2() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(inboxBroker.open("batch2")).thenReturn(writer2);

        match(tenantA, "/你好/hello/😄", MqttBroker, "inbox1", "batch1");
        match(tenantA, "/#", MqttBroker, "inbox1", "batch1");
        match(tenantA, "/#", InboxService, "inbox2", "batch2");

        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/你好/hello/😄", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/你好/hello/😄").intValue(), 3);

        ArgumentCaptor<DeliveryRequest> msgCap = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, after(1000).atMost(2)).deliver(msgCap.capture());
        int msgCount = 0;
        for (DeliveryRequest request : msgCap.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                DeliveryPackage deliveryPackage = request.getPackageMap().get(tenantId);
                assertEquals(tenantId, tenantA);
                for (DeliveryPack pack : deliveryPackage.getPackList()) {
                    TopicMessagePack msgPack = pack.getMessagePack();
                    Set<MatchInfo> subInfos = Sets.newHashSet(pack.getMatchInfoList());
                    assertEquals(msgPack.getTopic(), "/你好/hello/😄");
                    for (TopicMessagePack.PublisherPack publisherPack : msgPack.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                            msgCount += subInfos.size();
                        }
                    }
                }
            }
        }
        assertEquals(msgCount, 2);

        msgCap = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer2, timeout(1000).times(1)).deliver(msgCap.capture());
        for (DeliveryRequest request : msgCap.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                DeliveryPackage deliveryPackage = request.getPackageMap().get(tenantId);
                assertEquals(tenantId, tenantA);
                for (DeliveryPack pack : deliveryPackage.getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/你好/hello/😄");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }

        unmatch(tenantA, "/你好/hello/😄", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/#", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/#", InboxService, "inbox2", "batch2");
    }

    @Test(groups = "integration")
    public void testDistCase3() {
        // inline test
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        match(tenantA, "/a/b/c", MqttBroker, "inbox2", "batch1");
        await().until(() -> {
            try {
                reset(writer1);
                when(writer1.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
                BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
                assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 2);

                ArgumentCaptor<DeliveryRequest> list1 = ArgumentCaptor.forClass(DeliveryRequest.class);
                // the call is inlined
                verify(writer1, timeout(1).times(1)).deliver(list1.capture());
                log.info("Case3: verify writer1, list size is {}", list1.getAllValues().size());
                int msgCount = 0;
                Set<MatchInfo> matchInfos = new HashSet<>();
                for (DeliveryRequest request : list1.getAllValues()) {
                    for (String tenantId : request.getPackageMap().keySet()) {
                        for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                            TopicMessagePack msgs = pack.getMessagePack();
                            matchInfos.addAll(pack.getMatchInfoList());
                            assertEquals(msgs.getTopic(), "/a/b/c");
                            for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                                for (Message msg : publisherPack.getMessageList()) {
                                    msgCount++;
                                    assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                                }
                            }
                        }
                    }
                }
                return matchInfos.size() == 2 && msgCount == 1;
            } catch (Throwable e) {
                return false;
            }
        });
        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox2", "batch1");
    }

    @Test(groups = "integration")
    public void testDistCase4() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);

        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox1", "batch1");
        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c"), 1);
        }

        ArgumentCaptor<DeliveryRequest> list1 = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, after(1000).atMost(10)).deliver(list1.capture());
        for (DeliveryRequest request : list1.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }

        ArgumentCaptor<DeliveryRequest> list2 = ArgumentCaptor.forClass(DeliveryRequest.class);

        verify(writer2, after(200).atMost(10)).deliver(list2.capture());
        for (DeliveryRequest request : list2.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }
        assertEquals(list1.getAllValues().size() + list2.getAllValues().size(), 10);

        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
    }

    @Test(groups = "integration")
    public void testDistCase5() {
        Mockito.reset(distClient);
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);

        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox1", "batch1");
        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox2", "batch2");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 1);
        }

        ArgumentCaptor<DeliveryRequest> list1 = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, after(200).atMost(10)).deliver(list1.capture());

        ArgumentCaptor<DeliveryRequest> list2 = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer2, after(200).atMost(10)).deliver(list2.capture());

        List<DeliveryRequest> captured = list1.getAllValues().isEmpty() ? list2.getAllValues() : list1.getAllValues();

        for (DeliveryRequest request : captured) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }
        assertTrue(list1.getAllValues().isEmpty() || list2.getAllValues().isEmpty());

        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox2", "batch2");
    }

    @Test(groups = "integration")
    public void testDistCase6() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);
        when(mqttBroker.open("batch3")).thenReturn(writer3);

        match(tenantA, "/a/b/c", MqttBroker, "inbox6", "batch1");
        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox7", "batch2");
        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox8", "batch3");

        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 3);

        verify(writer1, timeout(1000).times(1)).deliver(any());
        verify(writer2, timeout(1000).times(1)).deliver(any());
        verify(writer3, timeout(1000).times(1)).deliver(any());

        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox6", "batch1");
        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox7", "batch2");
        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox8", "batch3");
    }

    @Test(groups = "integration")
    public void testDistCase7() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        match(tenantB, "#", MqttBroker, "inbox1", "batch1");
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 1);

        ArgumentCaptor<DeliveryRequest> list1 = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, timeout(1000).times(1)).deliver(list1.capture());
        int msgCount = 0;
        for (DeliveryRequest request : list1.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            msgCount++;
                            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                        }
                    }
                }
            }
        }
        assertEquals(msgCount, 1);

        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        unmatch(tenantB, "#", MqttBroker, "inbox1", "batch1");
    }

    @Test(groups = "integration")
    public void testRouteRefresh() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);
        when(mqttBroker.open("batch3")).thenReturn(writer3);

        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)]
        // expected behavior: inbox1 gets 1 message
        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        verify(writer1, timeout(1000).times(1)).deliver(any());

        // pub: qos0
        // topic: "/a/b/c"
        // action: delete inbox1 match record
        // sub: no sub
        // expected behavior: inbox1 gets no messages
        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        await().until(() -> {
            clearInvocations(writer1);
            dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            try {
                verify(writer1, timeout(1000).times(0)).deliver(any());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox2 join shared group
        // sub: inbox2 -> [($share/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets 1 message
        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
        await().until(() -> {
            clearInvocations(writer2);
            dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            try {
                verify(writer2, timeout(1000).times(1)).deliver(any());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox2 leaves the shared group and inbox3 joins
        // sub: inbox3 -> [($share/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets no messages and inbox3 gets 1
        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox2", "batch2");
        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox3", "batch3");
        await().until(() -> {
            clearInvocations(writer2, writer3);
            dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            try {
                verify(writer2, timeout(1000).times(0)).deliver(any());
                verify(writer3, timeout(1000).times(1)).deliver(any());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox2 joins an ordered shared group and inbox3 leaves the shared group
        // sub: inbox2 -> [($oshare/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets 1 message and inbox3 gets none
        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox3", "batch3");
        await().until(() -> {
            clearInvocations(writer2, writer3);
            dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            try {
                verify(writer2, timeout(1000).times(1)).deliver(any());
                verify(writer3, timeout(1000).times(0)).deliver(any());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        // pub: qos0
        // topic: "/a/b/c"
        // action: inbox3 joins the ordered shared group and inbox2 leaves
        // sub: inbox3 -> [($oshare/group/a/b/c, qos0)]
        // expected behavior: inbox2 gets no messages and inbox3 gets 1
        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox2", "batch2");
        match(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox3", "batch3");
        await().until(() -> {
            clearInvocations(writer2, writer3);
            dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            try {
                verify(writer2, timeout(1000).times(0)).deliver(any());
                verify(writer3, timeout(1000).times(1)).deliver(any());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        // clear
        unmatch(tenantA, "$oshare/group//a/b/c", MqttBroker, "inbox3", "batch3");
    }

    @Test(groups = "integration")
    public void testRouteRefreshWithWildcardTopic() {
        // pub: qos0
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)]
        // expected behavior: inbox1 gets 1 message
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);
        when(mqttBroker.open("batch3")).thenReturn(writer3);
        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "batch1");
        await().until(() -> {
            clearInvocations(writer1);
            dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            try {
                verify(writer1, timeout(1000).times(1)).deliver(any());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        // pub: qos0
        // topic: "/a/b/c", "/#"
        // action: inbox2 sub the wildcard topic "/#"
        // sub: inbox1 -> [(/a/b/c, qos0)], inbox2 -> [(/#, qos0)]
        // expected behavior: inbox1 gets 1 message and inbox2 gets 1 either
        match(tenantA, "/#", MqttBroker, "inbox2", "batch2");
        await().until(() -> {
            clearInvocations(writer1, writer2);
            dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            try {
                verify(writer1, timeout(1000).times(1)).deliver(any());
                verify(writer2, timeout(1000).times(1)).deliver(any());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        // pub: qos0
        // topic: "/a/b/c", "/#"
        // action: inbox3 joins the shared and ordered shared wildcard topic "/#"
        // sub: inbox1 -> [(/a/b/c, qos0)],
        // inbox2 -> [(/#, qos0)],
        // inbox3 -> [(#share/group/#, qos0), (#oshare/group/#, qos0)]
        // expected behavior: inbox1 and inbox2 gets 1 message, inbox3 gets 2
        match(tenantA, "$share/group/#", MqttBroker, "inbox3", "batch3");
        match(tenantA, "$oshare/group/#", MqttBroker, "inbox3", "batch3");
        // wait for cache refresh after writing
        await().until(() -> {
            clearInvocations(writer1, writer2, writer3);
            dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            try {
                verify(writer1, timeout(1000).times(1)).deliver(any());
                verify(writer2, timeout(1000).times(1)).deliver(any());
                verify(writer3, timeout(1000).times(2)).deliver(any());
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        // clear
        unmatch(tenantA, "/#", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "$share/group/#", MqttBroker, "inbox3", "batch3");
        unmatch(tenantA, "$oshare/group/#", MqttBroker, "inbox3", "batch3");
    }

    @Test(groups = "integration")
    public void testProbeAndSeek() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        match(tenantA, "test/#", MqttBroker, "inbox", "batch1");
        for (int i = 0; i < 21; i++) {
            match(tenantA, "test", MqttBroker, "inbox" + i, "batch1");
        }

        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "test/r1", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().getOrDefault("TopicB", 0).intValue(), 0);
        unmatch(tenantA, "TopicA/#", InboxService, "inbox1", "batch1");
    }

    @Test(groups = "integration")
    public void testOrderedShareWithGroups() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);

        match(tenantA, "$oshare/group1/#", MqttBroker, "inbox1", "batch1");
        match(tenantA, "$oshare/group2/#", MqttBroker, "inbox1", "batch1");
        Set<String> set = Set.of("$oshare/group1/#", "$oshare/group2/#");

        await().until(() -> {
            clearInvocations(writer1);
            Set<String> topicFilterSet = new HashSet<>();
            dist(tenantA, AT_MOST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            try {
                ArgumentCaptor<DeliveryRequest> captor = ArgumentCaptor.forClass(DeliveryRequest.class);
                verify(writer1, timeout(1000).times(2)).deliver(captor.capture());
                captor.getAllValues().forEach(req -> {
                    DeliveryPackage packs = req.getPackageMap().get(tenantA);
                    for (DeliveryPack pack : packs.getPackList()) {
                        for (MatchInfo matchInfo : pack.getMatchInfoList()) {
                            topicFilterSet.add(matchInfo.getMatcher().getMqttTopicFilter());
                        }
                    }
                });
                assertEquals(set, topicFilterSet);
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        unmatch(tenantA, "$oshare/group1/#", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "$oshare/group2/#", MqttBroker, "inbox1", "batch1");
    }
}