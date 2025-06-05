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

import static org.apache.bifromq.type.QoS.AT_LEAST_ONCE;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.dist.rpc.proto.BatchDistReply;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.TopicMessagePack;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Slf4j
public class DistQoS1Test extends DistWorkerTest {
    @AfterMethod(groups = "integration")
    public void clearMock() {
        Mockito.reset(writer1, writer2, writer3);
    }

    @Test(groups = "integration")
    public void succeedWithNoSub() {
        String topic = "/a/b/c";
        ByteString payload = copyFromUtf8("hello");

        BatchDistReply reply = dist(tenantA, AT_LEAST_ONCE, topic, payload, "orderKey1");
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().getOrDefault(topic, 0).intValue(), 0);
    }

    @Test(groups = "integration")
    public void testDistCase9() {
        // pub: qos1
        // topic: "/a/b/c"
        // sub: inbox1 -> [(/a/b/c, qos1)]
        // subBroker: inbox1 -> NO_INBOX
        // expected behavior: pub succeed with no sub
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("server1")).thenReturn(writer1);

        when(writer1.deliver(any())).thenAnswer(answer(DeliveryResult.Code.NO_SUB));

        when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(),
            anyInt(), anyLong())).thenReturn(
            CompletableFuture.completedFuture(null));

        match(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist(tenantA, AT_LEAST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 1);
        }

        ArgumentCaptor<DeliveryRequest> messageListCap = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, timeout(200).atLeastOnce()).deliver(messageListCap.capture());

        for (DeliveryRequest request : messageListCap.getAllValues()) {
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

        verify(distClient, timeout(200).atLeastOnce())
            .removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong());

        unmatch(tenantA, "/a/b/c", MqttBroker, "inbox1", "server1");
    }

    @SneakyThrows
    @Test(groups = "integration")
    public void testDistCase10() {
        // pub: qos1
        // topic: "/a/b/c"
        // sub: inbox1 -> [($share/group/a/b/c, qos1)]
        // subBroker: inbox1 -> NO_INBOX
        // expected behavior: pub succeed with no sub
        when(receiverManager.get(MqttBroker)).thenReturn(mqttBroker);
        when(mqttBroker.open("server1")).thenReturn(writer1);

        when(writer1.deliver(any())).thenAnswer(answer(DeliveryResult.Code.NO_SUB));
        when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(),
            anyLong()))
            .thenReturn(CompletableFuture.completedFuture(null));

        match(tenantA, "$share/group//a/b/c", MqttBroker, "inbox1", "server1");

        for (int i = 0; i < 10; i++) {
            BatchDistReply reply = dist(tenantA, AT_LEAST_ONCE, "/a/b/c", copyFromUtf8("Hello"), "orderKey1");
            assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/a/b/c").intValue(), 1);
        }

        ArgumentCaptor<DeliveryRequest> messageListCap = ArgumentCaptor.forClass(DeliveryRequest.class);
        verify(writer1, timeout(200).atLeastOnce()).deliver(messageListCap.capture());

        for (DeliveryRequest request : messageListCap.getAllValues()) {
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

        verify(distClient, timeout(200).atLeastOnce())
            .removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong());

        verify(eventCollector, timeout(200).atLeastOnce()).report(argThat(e -> e.type() == EventType.DELIVER_ERROR));

        unmatch(tenantA, "$share/group//a/b/c", MqttBroker, "inbox1", "server1");
    }
}
