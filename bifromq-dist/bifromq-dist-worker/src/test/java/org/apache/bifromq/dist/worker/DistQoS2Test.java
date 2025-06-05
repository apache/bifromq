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

import static org.apache.bifromq.type.QoS.EXACTLY_ONCE;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.dist.rpc.proto.BatchDistReply;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Slf4j
public class DistQoS2Test extends DistWorkerTest {
    @AfterMethod(groups = "integration")
    public void clearMock() {
        Mockito.reset(writer1, writer2, writer3);
        Mockito.reset(distClient);
    }

    @Test(groups = "integration")
    public void succeedWithNoSub() {
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(tenantA, EXACTLY_ONCE, topic, payload, "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().getOrDefault(topic, 0).intValue(), 0);
    }

    @Test(groups = "integration")
    public void distQoS2ToVariousSubQoS() {
        // pub: qos2
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos0),(/#, qos1)], inbox2 -> [(/#,qos2)]
        // expected behavior: inbox1 gets 2 messages, inbox2 get 1 message
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("server1")).thenReturn(writer1);
        when(mqttBroker.open("server2")).thenReturn(writer2);

        when(writer1.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
        when(writer2.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));

        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
        match(tenantA, "/#", MqttBroker, "inbox1", "server1");
        match(tenantA, "/#", MqttBroker, "inbox2", "server2");
        BatchDistReply reply = dist(tenantA, EXACTLY_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 3);

        ArgumentCaptor<DeliveryRequest> msgCap = ArgumentCaptor.forClass(DeliveryRequest.class);

        verify(writer1, timeout(200).atLeastOnce()).deliver(msgCap.capture());
        for (DeliveryRequest request : msgCap.getAllValues()) {
            for (String tenantId : request.getPackageMap().keySet()) {
                for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    Iterable<MatchInfo> subInfos = pack.getMatchInfoList();
                    assertEquals(msgs.getTopic(), "/a/b/c");
                    for (TopicMessagePack.PublisherPack publisherPack : msgs.getMessageList()) {
                        for (Message msg : publisherPack.getMessageList()) {
                            for (MatchInfo subInfo : subInfos) {
                                assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
                            }
                        }
                    }
                }
            }
        }

        msgCap = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer2, timeout(200).atLeastOnce()).deliver(msgCap.capture());

        assertEquals(msgCap.getAllValues().size(), 1);
        DeliveryRequest request = msgCap.getValue();
        for (String tenantId : request.getPackageMap().keySet()) {
            for (DeliveryPack pack : request.getPackageMap().get(tenantId).getPackList()) {
                TopicMessagePack inbox2Msgs = pack.getMessagePack();
                assertEquals(inbox2Msgs.getTopic(), "/a/b/c");
                TopicMessagePack.PublisherPack publisherPack = inbox2Msgs.getMessageList().iterator().next();
                Message msg = publisherPack.getMessage(0);
                assertEquals(msg.getPubQoS(), EXACTLY_ONCE);
                assertEquals(msg.getPayload(), copyFromUtf8("Hello"));
            }
        }

        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
        unmatch(tenantA, "/#", MqttBroker, "inbox1", "server1");
        unmatch(tenantA, "/#", MqttBroker, "inbox2", "server2");
    }
}
