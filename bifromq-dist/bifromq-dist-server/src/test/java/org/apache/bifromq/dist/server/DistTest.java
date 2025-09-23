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

package org.apache.bifromq.dist.server;

import static org.apache.bifromq.plugin.subbroker.TypeUtil.to;
import static org.apache.bifromq.plugin.subbroker.TypeUtil.toResult;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.dist.client.MatchResult;
import org.apache.bifromq.dist.client.PubResult;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryPackage;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.TopicUtil;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class DistTest extends DistServiceTest {
    private final String tenantId = "tenantA";

    @BeforeMethod(groups = "integration")
    public void resetMock() {
        Mockito.reset(inboxDeliverer);
    }

    @SneakyThrows
    @Test(groups = "integration", dependsOnMethods = "distWithFanOutSub")
    public void distWithNoSub() {
        Mockito.lenient().when(inboxDeliverer.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
        long reqId = System.nanoTime();
        ByteString payload = ByteString.EMPTY;
        ClientInfo clientInfo;
        int total = 10;
        CountDownLatch latch = new CountDownLatch(total);

        for (int i = 0; i < total; i++) {
            clientInfo = ClientInfo.newBuilder().setTenantId("trafficA").putMetadata("userId", "user" + i).build();
            try {
                PubResult result = distClient().pub(reqId, "/sport/tennis" + i,
                    Message.newBuilder().setPubQoS(QoS.AT_LEAST_ONCE).setPayload(payload)
                        .setExpiryInterval(Integer.MAX_VALUE).build(), clientInfo).join();
                assertEquals(result, PubResult.NO_MATCH);
            } catch (Throwable e) {
                fail();
            } finally {
                latch.countDown();
            }
        }
        latch.await();
    }

    @SneakyThrows
    @Test(groups = "integration")
    public void distWithSub() {
        Mockito.lenient().when(inboxDeliverer.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
        long reqId = System.nanoTime();
        ByteString payload = ByteString.EMPTY;
        int total = 1;
        for (int i = 0; i < total; i++) {
            distClient().addRoute(reqId, tenantId, TopicUtil.from("/sport/tennis" + i), "inbox" + i, "server1", 0, 1L)
                .join();
        }
        await().until(() -> {
            CountDownLatch latch = new CountDownLatch(total);
            for (int i = 0; i < total; i++) {
                ClientInfo clientInfo =
                    ClientInfo.newBuilder().setTenantId(tenantId).putMetadata("userId", "user" + i).build();
                try {
                    PubResult result = distClient().pub(reqId, "/sport/tennis" + i,
                        Message.newBuilder().setPubQoS(QoS.AT_LEAST_ONCE).setPayload(payload)
                            .setExpiryInterval(Integer.MAX_VALUE).build(), clientInfo).join();
                    assertEquals(result, PubResult.OK);
                } catch (Throwable e) {
                    return false;
                } finally {
                    latch.countDown();
                }
            }
            latch.await();
            return true;
        });
    }

    @SneakyThrows
    @Test(groups = "integration")
    public void distWithSharedSub() {
        Mockito.lenient().when(inboxDeliverer.deliver(any())).thenAnswer(answer(DeliveryResult.Code.OK));
        long reqId = System.nanoTime();
        ByteString payload = ByteString.EMPTY;
        int totalSub = 5;
        int totalMsg = 1;
        for (int i = 0; i < totalSub; i++) {
            MatchResult subResult =
                distClient().addRoute(reqId, tenantId, TopicUtil.from("$share/g1/sport/tennis" + i), "inbox" + i,
                        "server1", 0, 1L)
                    .join();
            assertEquals(subResult, MatchResult.OK);
        }
        await().until(() -> {
            CountDownLatch latch = new CountDownLatch(totalMsg);
            for (int i = 0; i < totalMsg; i++) {
                ClientInfo clientInfo =
                    ClientInfo.newBuilder().setTenantId(tenantId).putMetadata("userId", "user" + i).build();
                try {
                    PubResult result = distClient().pub(reqId, "sport/tennis" + i,
                        Message.newBuilder().setPubQoS(QoS.AT_LEAST_ONCE).setPayload(payload)
                            .setTimestamp(HLC.INST.get()).setExpiryInterval(Integer.MAX_VALUE).build(),
                        clientInfo).join();
                    assertEquals(result, PubResult.OK);
                } catch (Throwable e) {
                    return false;
                } finally {
                    latch.countDown();
                }
            }
            latch.await();
            return true;
        });
    }

    @SneakyThrows
    @Test(groups = "integration")
    public void distWithFanOutSub() {
        List<DeliveryRequest> capturedArguments = new CopyOnWriteArrayList<>();
        when(inboxDeliverer.deliver(any())).thenAnswer((Answer<CompletableFuture<DeliveryReply>>) invocation -> {
            DeliveryRequest request = invocation.getArgument(0);
            // the argument object will be reused, so make a clone
            capturedArguments.add(request);
            Map<String, Map<MatchInfo, DeliveryResult.Code>> resultMap = new HashMap<>();
            for (String tenantId : request.getPackageMap().keySet()) {
                Map<MatchInfo, DeliveryResult.Code> r = resultMap.computeIfAbsent(tenantId, k -> new HashMap<>());
                for (DeliveryPack inboxWrite : request.getPackageMap().get(tenantId).getPackList()) {
                    for (MatchInfo matchInfo : inboxWrite.getMatchInfoList()) {
                        r.put(matchInfo, DeliveryResult.Code.OK);
                    }
                }
            }
            return CompletableFuture.completedFuture(
                DeliveryReply.newBuilder().setCode(DeliveryReply.Code.OK).putAllResult(toResult(resultMap)).build());
        });

        long reqId = System.nanoTime();
        ByteString payload = ByteString.EMPTY;
        int totalInbox = 100;
        for (int i = 0; i < totalInbox; i++) {
            distClient().addRoute(reqId, tenantId, TopicUtil.from("/sport/tennis"), "inbox" + i, "server1", 0, 1L)
                .join();
        }

        int totalPub = 2;
        await().forever().until(() -> {
            try {
                ClientInfo pubClient =
                    ClientInfo.newBuilder().setTenantId(tenantId).putMetadata("userId", "pubUser").build();
                for (int i = 0; i < totalPub; i++) {
                    PubResult result = distClient().pub(reqId, "/sport/tennis",
                        Message.newBuilder().setPubQoS(QoS.AT_LEAST_ONCE).setPayload(payload)
                            .setExpiryInterval(Integer.MAX_VALUE).build(), pubClient).join();
                    assertEquals(result, PubResult.OK);
                }
                return true;
            } catch (Throwable e) {
                return false;
            }
        });

        Thread.sleep(100);

        Set<MatchInfo> matchInfos = new HashSet<>();
        int msgCount = 0;
        for (DeliveryRequest writeReq : capturedArguments) {
            for (String tenantId : writeReq.getPackageMap().keySet()) {
                for (DeliveryPack pack : writeReq.getPackageMap().get(tenantId).getPackList()) {
                    TopicMessagePack msgs = pack.getMessagePack();
                    Set<MatchInfo> inboxes = Sets.newHashSet(pack.getMatchInfoList());
                    matchInfos.addAll(inboxes);
                    msgCount += msgs.getMessageCount() * inboxes.size();
                }
            }
        }
        assertEquals(matchInfos.size(), totalInbox);
        assertEquals(msgCount, totalInbox * totalPub);
    }

    private Answer<CompletableFuture<DeliveryReply>> answer(DeliveryResult.Code code) {
        return invocation -> {
            DeliveryRequest request = invocation.getArgument(0);
            DeliveryReply.Builder replyBuilder = DeliveryReply.newBuilder();
            for (Map.Entry<String, DeliveryPackage> entry : request.getPackageMap().entrySet()) {
                String tenantId = entry.getKey();
                Map<MatchInfo, DeliveryResult.Code> resultMap = new HashMap<>();
                for (DeliveryPack pack : entry.getValue().getPackList()) {
                    for (MatchInfo subInfo : pack.getMatchInfoList()) {
                        resultMap.put(subInfo, code);
                    }
                }
                replyBuilder.setCode(DeliveryReply.Code.OK).putResult(tenantId, to(resultMap));
            }
            return CompletableFuture.completedFuture(replyBuilder.build());
        };
    }
}
