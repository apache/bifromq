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

import static org.apache.bifromq.basekv.client.KVRangeRouterUtil.findByKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basekv.client.KVRangeSetting;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.dist.client.UnmatchResult;
import org.apache.bifromq.dist.rpc.proto.BatchMatchReply;
import org.apache.bifromq.dist.rpc.proto.BatchUnmatchReply;
import org.apache.bifromq.dist.rpc.proto.DistServiceROCoProcInput;
import org.apache.bifromq.dist.rpc.proto.GCReply;
import org.apache.bifromq.dist.rpc.proto.GCRequest;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.plugin.subbroker.CheckRequest;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DistWorkerGCTest extends DistWorkerTest {
    @BeforeMethod
    public void routeDeletesToStore() {
        // Keep the real SubscriptionCleaner and store mutation, replacing only DistClient RPC routing.
        when(distClient.removeRoute(anyLong(), anyString(), any(), anyString(), anyString(), anyInt(), anyLong()))
            .thenAnswer(inv -> {
                String tenant = inv.getArgument(1);
                RouteMatcher matcher = inv.getArgument(2);
                String receiver = inv.getArgument(3);
                String deliverer = inv.getArgument(4);
                int broker = inv.getArgument(5);
                long incarnation = inv.getArgument(6);
                return CompletableFuture.supplyAsync(() -> {
                    BatchUnmatchReply.TenantBatch.Code result = unmatch(tenant, matcher.getMqttTopicFilter(),
                        broker, receiver, deliverer, incarnation);
                    return result == BatchUnmatchReply.TenantBatch.Code.OK ? UnmatchResult.OK : UnmatchResult.NOT_EXISTED;
                });
            });
    }

    @DataProvider
    public Object[][] checks() {
        return new Object[][] {
            {"a/b", CheckReply.Code.NO_SUB, true}, {"a/b", CheckReply.Code.NO_RECEIVER, true},
            {"a/b", CheckReply.Code.OK, false}, {"a/b", CheckReply.Code.ERROR, false},
            {"a/b", CheckReply.Code.TRY_LATER, false},
            {"$share/g/a/b", CheckReply.Code.NO_SUB, true},
            {"$share/g/a/b", CheckReply.Code.NO_RECEIVER, true},
            {"$share/g/a/b", CheckReply.Code.OK, false}, {"$share/g/a/b", CheckReply.Code.ERROR, false},
            {"$share/g/a/b", CheckReply.Code.TRY_LATER, false}
        };
    }

    // A completed check may prove a route absent, or be unable to decide while the receiver is unavailable.
    @Test(groups = "integration", dataProvider = "checks")
    public void gcDeletesOnlyConfirmedGarbage(String topic, CheckReply.Code code, boolean deleted) throws Exception {
        assertEquals(match(tenantA, topic, MqttBroker, "receiver", "deliverer", 7L), BatchMatchReply.TenantBatch.Code.OK);
        when(mqttBroker.check(any(CheckRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckReply.newBuilder().addCode(code).build()));
        GCReply reply = gc(topic).get(10, TimeUnit.SECONDS);
        assertEquals(reply.getInspectedCount(), 1);
        assertEquals(reply.getRemoveSuccess(), deleted ? 1 : 0);
        // Querying the actual store mutation result distinguishes deletion from a successful check alone.
        assertEquals(unmatch(tenantA, topic, MqttBroker, "receiver", "deliverer", 7L), deleted
            ? BatchUnmatchReply.TenantBatch.Code.NOT_EXISTED : BatchUnmatchReply.TenantBatch.Code.OK);
    }

    @DataProvider
    public Object[][] topics() {
        return new Object[][] {{"a/b"}, {"$share/g/a/b"}};
    }

    // A client can resubscribe while a Check RPC for its previous incarnation is still in flight.
    @Test(groups = "integration", dataProvider = "topics")
    public void gcPreservesSubscriptionRenewedDuringCheck(String topic) throws Exception {
        assertEquals(match(tenantA, topic, MqttBroker, "receiver", "deliverer", 7L), BatchMatchReply.TenantBatch.Code.OK);
        CountDownLatch checking = new CountDownLatch(1);
        CompletableFuture<CheckReply> check = new CompletableFuture<>();
        when(mqttBroker.check(any(CheckRequest.class))).thenAnswer(inv -> {
            checking.countDown();
            return check;
        });
        CompletableFuture<GCReply> pending = gc(topic);
        try {
            assertTrue(checking.await(10, TimeUnit.SECONDS));
            assertEquals(match(tenantA, topic, MqttBroker, "receiver", "deliverer", 8L),
                BatchMatchReply.TenantBatch.Code.OK);
        } finally {
            check.complete(CheckReply.newBuilder().addCode(CheckReply.Code.NO_SUB).build());
        }
        assertEquals(pending.get(10, TimeUnit.SECONDS).getRemoveSuccess(), 0);
        assertEquals(unmatch(tenantA, topic, MqttBroker, "receiver", "deliverer", 8L),
            BatchUnmatchReply.TenantBatch.Code.OK);
    }

    private CompletableFuture<GCReply> gc(String topic) {
        RouteMatcher matcher = TopicUtil.from(topic);
        ByteString key = matcher.getType() == RouteMatcher.Type.Normal
            ? toNormalRouteKey(tenantA, matcher, "0\0receiver\0deliverer") : toGroupRouteKey(tenantA, matcher);
        KVRangeSetting range = findByKey(key, storeClient.latestEffectiveRouter()).orElseThrow();
        return storeClient.query(range.leader(), KVRangeRORequest.newBuilder()
            .setReqId(1).setVer(range.ver()).setKvRangeId(range.id())
            .setRoCoProc(ROCoProcInput.newBuilder().setDistService(DistServiceROCoProcInput.newBuilder()
                .setGc(GCRequest.newBuilder().setReqId(1).setStartKey(key).setStepHint(1).setScanQuota(1))))
            .build()).thenApply(reply -> {
                assertEquals(reply.getCode(), ReplyCode.Ok);
                return reply.getRoCoProcResult().getDistService().getGc();
            });
    }
}
