/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.dist.client.PubResult.BACK_PRESSURE_REJECTED;
import static org.apache.bifromq.dist.client.PubResult.NO_MATCH;
import static org.apache.bifromq.dist.client.PubResult.OK;
import static org.apache.bifromq.plugin.settingprovider.Setting.RetainEnabled;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainMessageSpaceBytes;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalRetainTopics;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.storage.proto.BatchAttachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchDetachRequest;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTReply;
import org.apache.bifromq.inbox.storage.proto.BatchSendLWTRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import org.apache.bifromq.inbox.storage.proto.LWT;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDistError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.disthandling.WillDisted;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetained;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.MsgRetainedError;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling.RetainMsgCleared;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import com.google.protobuf.ByteString;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SendLWTEventTest extends InboxStoreTest {
    private long now;
    private String tenantId;
    private String inboxId;
    private long incarnation;
    private long modAfterDetach;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void beforeCastStart(Method method) {
        super.beforeCastStart(method);
        reset(eventCollector);
    }

    @Test(groups = "integration")
    public void sendLWTDistOkOrNoMatchFiresWillDisted() {
        long now = HLC.INST.getPhysical();
        setupAttachAndDetach(now);

        when(distClient.pub(anyLong(), anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(OK));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId))).thenReturn(false);

        requestSendAndVerify();

        verify(eventCollector).report(argThat(e -> e instanceof WillDisted));

        reset(eventCollector);
        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(NO_MATCH));

        requestSendAndVerify();
        verify(eventCollector).report(argThat(e -> e instanceof WillDisted));
    }

    @Test(groups = "integration")
    public void sendLWTDistBackPressureFiresWillDistError() {
        long now = HLC.INST.getPhysical();
        setupAttachAndDetach(now);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(BACK_PRESSURE_REJECTED));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId))).thenReturn(false);

        requestSendAndVerify();

        verify(eventCollector).report(argThat(e -> e instanceof WillDistError));
    }

    @Test(groups = "integration")
    public void sendLWTRetainRetainedFiresMsgRetained() {
        long now = HLC.INST.getPhysical();
        setupAttachAndDetach(now);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(OK));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId))).thenReturn(true);
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(retainClient.retain(anyLong(), any(), any(), any(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                RetainReply.newBuilder().setResult(RetainReply.Result.RETAINED).build()));

        requestSendAndVerify();

        verify(eventCollector).report(argThat(e -> e instanceof MsgRetained));
    }

    @Test(groups = "integration")
    public void sendLWTRetainClearedFiresRetainMsgCleared() {
        long now = HLC.INST.getPhysical();
        setupAttachAndDetach(now);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(OK));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId))).thenReturn(true);
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(retainClient.retain(anyLong(), any(), any(), any(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                RetainReply.newBuilder().setResult(RetainReply.Result.CLEARED).build()));

        requestSendAndVerify();

        verify(eventCollector).report(argThat(e -> e instanceof RetainMsgCleared));
    }

    @Test(groups = "integration", dataProvider = "retainErrorResults")
    public void sendLWTRetainErrorFiresMsgRetainedError(RetainReply.Result result) {
        long now = HLC.INST.getPhysical();
        setupAttachAndDetach(now);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(OK));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId))).thenReturn(true);
        when(resourceThrottler.hasResource(anyString(), any())).thenReturn(true);
        when(retainClient.retain(anyLong(), any(), any(), any(), anyInt(), any()))
            .thenReturn(CompletableFuture.completedFuture(
                RetainReply.newBuilder().setResult(result).build()));

        requestSendAndVerify();

        verify(eventCollector).report(argThat(e -> e instanceof MsgRetainedError));
    }

    @DataProvider(name = "retainErrorResults")
    public Object[][] retainErrorResults() {
        return new Object[][] {
            {RetainReply.Result.BACK_PRESSURE_REJECTED},
            {RetainReply.Result.EXCEED_LIMIT},
            {RetainReply.Result.ERROR}
        };
    }

    @Test(groups = "integration")
    public void sendLWTRetainDisabledFiresMsgRetainedError() {
        long now = HLC.INST.getPhysical();
        setupAttachAndDetach(now);

        when(distClient.pub(anyLong(), anyString(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(OK));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId))).thenReturn(false);

        requestSendAndVerify();

        verify(eventCollector).report(argThat(e -> e instanceof MsgRetainedError));
    }

    private void requestSendAndVerify() {
        BatchSendLWTRequest.Params p = BatchSendLWTRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setVersion(InboxVersion.newBuilder()
                .setIncarnation(incarnation)
                .setMod(modAfterDetach)
                .build())
            .setNow(now)
            .build();
        assertEquals(requestSendLWT(p).get(0), BatchSendLWTReply.Code.OK);
    }

    private void setupAttachAndDetach(long now) {
        this.now = now;
        this.tenantId = "tenant-" + System.nanoTime();
        this.inboxId = "inbox-" + System.nanoTime();
        this.incarnation = System.nanoTime();
        ClientInfo client = ClientInfo.newBuilder().setTenantId(tenantId).build();

        BatchAttachRequest.Params.Builder attachB = BatchAttachRequest.Params.newBuilder()
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setExpirySeconds(5)
            .setClient(client)
            .setLwt(LWT.newBuilder()
                .setTopic("t")
                .setDelaySeconds(1)
                .setMessage(Message.newBuilder()
                    .setPayload(ByteString.copyFromUtf8("p"))
                    .setIsRetain(true)
                    .build())
                .build())
            .setNow(now);
        InboxVersion inboxVersion = requestAttach(attachB.build()).get(0);

        BatchDetachRequest.Params d = BatchDetachRequest.Params.newBuilder()
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setExpirySeconds(10)
            .setVersion(inboxVersion)
            .setNow(now)
            .build();
        requestDetach(d);
        this.modAfterDetach = inboxVersion.getMod() + (1);
    }

    @Test(groups = "integration")
    public void sendLWTNoTotalRetainTopicsFiresOutOfTenantResource() {
        long now = HLC.INST.getPhysical();
        setupAttachAndDetach(now);

        when(distClient.pub(anyLong(), anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(OK));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId))).thenReturn(true);
        when(resourceThrottler.hasResource(eq(tenantId), eq(TotalRetainTopics))).thenReturn(false);

        requestSendAndVerify();

        verify(eventCollector).report(argThat(e ->
            e instanceof OutOfTenantResource && ((OutOfTenantResource) e).reason().equals(TotalRetainTopics.name())
        ));
    }

    @Test(groups = "integration")
    public void sendLWTNoRetainMessageSpaceFiresOutOfTenantResource() {
        long now = HLC.INST.getPhysical();
        setupAttachAndDetach(now);

        when(distClient.pub(anyLong(), anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(OK));
        when(settingProvider.provide(eq(RetainEnabled), eq(tenantId))).thenReturn(true);
        when(resourceThrottler.hasResource(eq(tenantId), eq(TotalRetainTopics))).thenReturn(true);
        when(resourceThrottler.hasResource(eq(tenantId), eq(TotalRetainMessageSpaceBytes))).thenReturn(false);

        requestSendAndVerify();

        verify(eventCollector).report(argThat(e ->
            e instanceof OutOfTenantResource
                && ((OutOfTenantResource) e).reason().equals(TotalRetainMessageSpaceBytes.name())
        ));
    }
}
