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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.dist.rpc.proto.BatchDistReply;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.distservice.GroupFanoutThrottled;
import org.apache.bifromq.plugin.eventcollector.distservice.PersistentFanoutBytesThrottled;
import org.apache.bifromq.plugin.eventcollector.distservice.PersistentFanoutThrottled;
import org.apache.bifromq.plugin.resourcethrottler.TenantResourceType;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.testng.annotations.Test;

public class FanoutThrottledTest extends DistWorkerTest {
    @Test(groups = "integration")
    public void onePassPersistentFanoutLimit() {
        when(inboxBroker.open("batch1")).thenReturn(writer1);
        when(inboxBroker.open("batch2")).thenReturn(writer2);
        when(inboxBroker.open("batch3")).thenReturn(writer3);
        when(writer1.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer2.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer3.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));

        // match 3 persistent fanout subscriptions
        match(tenantA, "/fanout/topic", InboxService, "inbox1", "batch1");
        match(tenantA, "/fanout/topic", InboxService, "inbox2", "batch2");
        match(tenantA, "/fanout/topic", InboxService, "inbox3", "batch3");

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), any())).thenReturn(2);

        // trigger dist
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/fanout/topic", copyFromUtf8("throttled"), "orderKey1");

        // verify reply shows fanout = 2
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/fanout/topic").intValue(), 2);
        // cleanup
        unmatch(tenantA, "/fanout/topic", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/fanout/topic", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "/fanout/topic", MqttBroker, "inbox3", "batch3");
    }

    @Test(groups = "integration")
    public void deliverPersistentFanoutThrottled() {
        when(inboxBroker.open("batch1")).thenReturn(writer1);
        when(inboxBroker.open("batch2")).thenReturn(writer2);
        when(inboxBroker.open("batch3")).thenReturn(writer3);
        when(writer1.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer2.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer3.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));

        // match 3 persistent fanout subscriptions
        match(tenantA, "/fanout/topic2", InboxService, "inbox1", "batch1");
        match(tenantA, "/fanout/topic2", InboxService, "inbox2", "batch2");
        match(tenantA, "/fanout/topic2", InboxService, "inbox3", "batch3");
        // trigger dist
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/fanout/topic2", copyFromUtf8("throttled"), "orderKey1");
        // verify reply shows fanout = 3
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/fanout/topic2").intValue(), 3);

        // set max persistent fanout = 2
        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), any())).thenReturn(2);
        when(settingProvider.provide(eq(Setting.MaxPersistentFanoutBytes), any())).thenReturn(Long.MAX_VALUE);

        // trigger dist and cache will be used
        dist(tenantA, AT_MOST_ONCE, "/fanout/topic2", copyFromUtf8("throttled"), "orderKey1");

        verify(eventCollector, atLeast(1)).report(argThat(e -> e.type() == EventType.PERSISTENT_FANOUT_THROTTLED
            && ((PersistentFanoutThrottled) e).tenantId().equals(tenantA)
            && ((PersistentFanoutThrottled) e).topic().equals("/fanout/topic2")
            && ((PersistentFanoutThrottled) e).maxCount() == 2));
        // cleanup
        unmatch(tenantA, "/fanout/topic2", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/fanout/topic2", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "/fanout/topic2", MqttBroker, "inbox3", "batch3");
    }

    @Test(groups = "integration")
    public void deliverPersistentFanoutBytesThrottled() {
        when(inboxBroker.open("batch1")).thenReturn(writer1);
        when(inboxBroker.open("batch2")).thenReturn(writer2);
        when(inboxBroker.open("batch3")).thenReturn(writer3);
        when(writer1.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer2.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer3.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));

        // match 3 persistent fanout subscriptions
        match(tenantA, "/fanout/topic", InboxService, "inbox1", "batch1");
        match(tenantA, "/fanout/topic", InboxService, "inbox2", "batch2");
        match(tenantA, "/fanout/topic", InboxService, "inbox3", "batch3");
        // trigger dist
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/fanout/topic", copyFromUtf8("throttled"), "orderKey1");
        // verify reply shows fanout = 3
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/fanout/topic").intValue(), 3);

        when(settingProvider.provide(eq(Setting.MaxPersistentFanout), any())).thenReturn(Integer.MAX_VALUE);
        when(settingProvider.provide(eq(Setting.MaxPersistentFanoutBytes), any())).thenReturn(10L);

        // trigger dist and cache will be used
        dist(tenantA, AT_MOST_ONCE, "/fanout/topic", copyFromUtf8("throttled"), "orderKey1");

        verify(eventCollector).report(argThat(e -> e.type() == EventType.PERSISTENT_FANOUT_BYTES_THROTTLED
            && ((PersistentFanoutBytesThrottled) e).tenantId().equals(tenantA)
            && ((PersistentFanoutBytesThrottled) e).topic().equals("/fanout/topic")
            && ((PersistentFanoutBytesThrottled) e).maxBytes() == 10));
        // cleanup
        unmatch(tenantA, "/fanout/topic", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/fanout/topic", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "/fanout/topic", MqttBroker, "inbox3", "batch3");
    }

    @Test(groups = "integration")
    public void onePassGroupFanoutLimit() {
        when(inboxBroker.open("batch1")).thenReturn(writer1);
        when(inboxBroker.open("batch2")).thenReturn(writer2);
        when(inboxBroker.open("batch3")).thenReturn(writer3);
        when(writer1.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer2.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer3.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));

        // match 3 group fanout subscriptions
        match(tenantA, "$share/group1/fanout/topic", InboxService, "inbox1", "batch1");
        match(tenantA, "$share/group2/fanout/topic", InboxService, "inbox2", "batch2");
        match(tenantA, "$share/group3/fanout/topic", InboxService, "inbox3", "batch3");

        when(settingProvider.provide(eq(Setting.MaxGroupFanout), any())).thenReturn(2);

        // trigger dist
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "fanout/topic", copyFromUtf8("throttled"), "orderKey1");

        // verify reply shows fanout = 2
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("fanout/topic").intValue(), 2);
        // cleanup
        unmatch(tenantA, "$share/group1/fanout/topic", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "$share/group2/fanout/topic", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "$share/group3/fanout/topic", MqttBroker, "inbox3", "batch3");
    }

    @Test(groups = "integration")
    public void deliverGroupFanoutThrottled() {
        when(inboxBroker.open("batch1")).thenReturn(writer1);
        when(inboxBroker.open("batch2")).thenReturn(writer2);
        when(inboxBroker.open("batch3")).thenReturn(writer3);
        when(writer1.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer2.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer3.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));

        // match 3 group fanout subscriptions
        match(tenantA, "$share/group1/fanout/topic", InboxService, "inbox1", "batch1");
        match(tenantA, "$share/group2/fanout/topic", InboxService, "inbox2", "batch2");
        match(tenantA, "$share/group3/fanout/topic", InboxService, "inbox3", "batch3");
        // trigger dist
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "fanout/topic", copyFromUtf8("throttled"), "orderKey1");
        // verify reply shows fanout = 3
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("fanout/topic").intValue(), 3);

        // set max group fanout = 2
        when(settingProvider.provide(eq(Setting.MaxGroupFanout), any())).thenReturn(2);

        // trigger dist and cache will be used
        dist(tenantA, AT_MOST_ONCE, "fanout/topic", copyFromUtf8("throttled"), "orderKey1");

        verify(eventCollector).report(argThat(e -> e.type() == EventType.GROUP_FANOUT_THROTTLED
            && ((GroupFanoutThrottled) e).tenantId().equals(tenantA)
            && ((GroupFanoutThrottled) e).topic().equals("fanout/topic")
            && ((GroupFanoutThrottled) e).maxCount() == 2));
        // cleanup
        unmatch(tenantA, "$share/group1/fanout/topic", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "$share/group2/fanout/topic", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "$share/group3/fanout/topic", MqttBroker, "inbox3", "batch3");
    }

    @Test(groups = "integration")
    public void deliverNoPersistentFanoutBandwidth() {
        when(inboxBroker.open("batch1")).thenReturn(writer1);
        when(inboxBroker.open("batch2")).thenReturn(writer2);
        when(inboxBroker.open("batch3")).thenReturn(writer3);
        when(writer1.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer2.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer3.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));

        // match 3 persistent fanout subscriptions
        match(tenantA, "/fanout/topic2", InboxService, "inbox1", "batch1");
        match(tenantA, "/fanout/topic2", InboxService, "inbox2", "batch2");
        match(tenantA, "/fanout/topic2", InboxService, "inbox3", "batch3");

        // set no persistent fanout bandwidth
        when(resourceThrottler.hasResource(eq(tenantA), eq(TenantResourceType.TotalPersistentFanOutBytesPerSeconds)))
            .thenReturn(false);
        // trigger dist
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/fanout/topic2", copyFromUtf8("throttled"), "orderKey1");
        // verify reply shows fanout = 3
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/fanout/topic2").intValue(), 3);

        verify(eventCollector, atLeast(1))
            .report(argThat(e -> e.type() == EventType.OUT_OF_TENANT_RESOURCE
                && ((OutOfTenantResource) e).reason().equals("TotalPersistentFanOutBytesPerSeconds")));
        // cleanup
        unmatch(tenantA, "/fanout/topic2", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/fanout/topic2", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "/fanout/topic2", MqttBroker, "inbox3", "batch3");
    }

    @Test(groups = "integration")
    public void deliverNoTransientFanoutBandwidth() {
        when(mqttBroker.open("batch1")).thenReturn(writer1);
        when(mqttBroker.open("batch2")).thenReturn(writer2);
        when(mqttBroker.open("batch3")).thenReturn(writer3);
        when(writer1.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer2.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));
        when(writer3.deliver(any())).thenReturn(CompletableFuture.completedFuture(DeliveryReply.getDefaultInstance()));

        // match 3 transient fanout subscriptions
        match(tenantA, "/fanout/topic2", MqttBroker, "inbox1", "batch1");
        match(tenantA, "/fanout/topic2", MqttBroker, "inbox2", "batch2");
        match(tenantA, "/fanout/topic2", MqttBroker, "inbox3", "batch3");

        // set no persistent fanout bandwidth
        when(resourceThrottler.hasResource(eq(tenantA), eq(TenantResourceType.TotalTransientFanOutBytesPerSeconds)))
            .thenReturn(false);
        // trigger dist
        BatchDistReply reply = dist(tenantA, AT_MOST_ONCE, "/fanout/topic2", copyFromUtf8("throttled"), "orderKey1");
        // verify reply shows fanout = 3
        assertEquals(reply.getResultMap().get(tenantA).getFanoutMap().get("/fanout/topic2").intValue(), 3);

        verify(eventCollector, atLeast(1))
            .report(argThat(e -> e.type() == EventType.OUT_OF_TENANT_RESOURCE
                && ((OutOfTenantResource) e).reason().equals("TotalTransientFanOutBytesPerSeconds")));
        // cleanup
        unmatch(tenantA, "/fanout/topic2", MqttBroker, "inbox1", "batch1");
        unmatch(tenantA, "/fanout/topic2", MqttBroker, "inbox2", "batch2");
        unmatch(tenantA, "/fanout/topic2", MqttBroker, "inbox3", "batch3");
    }
}
