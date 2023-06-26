/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.dist.worker;

import static com.baidu.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.baidu.bifromq.type.QoS.AT_MOST_ONCE;
import static com.baidu.bifromq.type.QoS.EXACTLY_ONCE;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.baidu.bifromq.dist.rpc.proto.BatchDistReply;
import com.baidu.bifromq.plugin.inboxbroker.InboxPack;
import com.baidu.bifromq.plugin.inboxbroker.WriteResult;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

@Slf4j
public class DistQoS2Test extends DistWorkerTest {
    @Test(groups = "integration")
    public void succeedWithNoSub() {
        String trafficId = "trafficA";
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(trafficId, EXACTLY_ONCE, topic, payload, "orderKey1");
        assertTrue(reply.getResultMap().get(trafficId).getFanoutMap().getOrDefault(topic, 0).intValue() == 0);
    }

    @Test(groups = "integration")
    public void distQoS2ToVariousSubQoS() {
        // pub: qos2
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos1)], inbox2 -> [(/#,qos2)]
        // expected behavior: inbox1 gets 2 messages, inbox2 get 1 message
        when(receiverManager.openWriter("server1", MqttBroker))
            .thenReturn(writer1);
        when(receiverManager.openWriter("server2", MqttBroker))
            .thenReturn(writer2);
        when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });
        when(writer2.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });

        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "server1");
        insertMatchRecord("trafficA", "/#", AT_LEAST_ONCE,
            MqttBroker, "inbox1", "server1");
        insertMatchRecord("trafficA", "/#", EXACTLY_ONCE,
            MqttBroker, "inbox2", "server2");
        BatchDistReply reply =
            dist("trafficA", EXACTLY_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);


        ArgumentCaptor<Iterable<InboxPack>> msgCap = ArgumentCaptor.forClass(Iterable.class);

        verify(writer1, timeout(100).atLeastOnce()).write(msgCap.capture());
        for (InboxPack pack : msgCap.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            Iterable<SubInfo> subInfos = pack.inboxes;
            assertEquals(msgs.getTopic(), "/a/b/c");
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    for (SubInfo subInfo : subInfos) {
                        assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                    }
                }
            }
        }

//        // '/#' must come first
//        List<TopicMessage> inbox1Msgs = msgCap.getValue().get("trafficA").get("inbox1");
//        assertEquals(inbox1Msgs.get(0).getSubQoS(), AT_LEAST_ONCE);
//        assertEquals(inbox1Msgs.get(1).getSubQoS(), AT_MOST_ONCE);

        msgCap = ArgumentCaptor.forClass(Iterable.class);
        verify(writer2, timeout(100).atLeastOnce()).write(msgCap.capture());

        assertEquals(msgCap.getAllValues().size(), 1);
        for (InboxPack pack : msgCap.getValue()) {
            TopicMessagePack inbox2Msgs = pack.messagePack;
            assertEquals(inbox2Msgs.getTopic(), "/a/b/c");
            TopicMessagePack.SenderMessagePack senderMsgPack = inbox2Msgs.getMessageList().iterator().next();
            Message msg = senderMsgPack.getMessage(0);
            assertEquals(msg.getPubQoS(), EXACTLY_ONCE);
            assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
        }
    }

    @Test(groups = "integration")
    public void distQoS2ToErrorInbox() {
        // pub: qos2
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0)]
        // subBroker: inbox1 -> ERROR
        // expected behavior: pub succeed

        when(receiverManager.openWriter("server1", MqttBroker))
            .thenReturn(writer1);
        when(writer1.write(any()))
            .thenAnswer((Answer<CompletableFuture<Map<SubInfo, WriteResult>>>) invocation -> {
                Iterable<InboxPack> inboxPacks = invocation.getArgument(0);
                Map<SubInfo, WriteResult> resultMap = new HashMap<>();
                for (InboxPack inboxWrite : inboxPacks) {
                    for (SubInfo subInfo : inboxWrite.inboxes) {
                        resultMap.put(subInfo, ThreadLocalRandom.current().nextDouble() <= 0.5 ? WriteResult.error(
                            new RuntimeException()) : WriteResult.OK);
                    }
                }
                return CompletableFuture.completedFuture(resultMap);
            });
        insertMatchRecord("trafficA", "/a/b/c", AT_MOST_ONCE,
            MqttBroker, "inbox1", "server1");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply =
                dist("trafficA", EXACTLY_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertTrue(reply.getResultMap().get("trafficA").getFanoutMap().get("/a/b/c").intValue() > 0);
        }

        ArgumentCaptor<Iterable<InboxPack>> messageListCap = ArgumentCaptor.forClass(Iterable.class);

        verify(writer1, timeout(100).atLeastOnce()).write(messageListCap.capture());
        verify(writer1, after(100).atMost(10)).write(messageListCap.capture());
        for (InboxPack pack : messageListCap.getValue()) {
            TopicMessagePack msgs = pack.messagePack;
            assertEquals(msgs.getTopic(), "/a/b/c");
            for (TopicMessagePack.SenderMessagePack senderMsgPack : msgs.getMessageList()) {
                for (Message msg : senderMsgPack.getMessageList()) {
                    assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                }
            }
        }
    }
}
