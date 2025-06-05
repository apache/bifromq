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

package org.apache.bifromq.sessiondict.client;

import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.baserpc.client.IRPCClient;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.bifromq.sessiondict.rpc.proto.Quit;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.sessiondict.rpc.proto.Session;
import org.apache.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import org.apache.bifromq.type.ClientInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SessionRegisterTest {
    private final String tenantId = "tenantId";
    private final String registryKey = "registryKey";
    private final String userId = "userId";
    private final String clientId = "clientId";
    private final ClientInfo owner = ClientInfo.newBuilder()
        .setTenantId(tenantId)
        .setType(MQTT_TYPE_VALUE)
        .putMetadata(MQTT_USER_ID_KEY, userId)
        .putMetadata(MQTT_CLIENT_ID_KEY, clientId)
        .build();
    private AutoCloseable closeable;
    @Mock
    private ISessionDictClient.IKillListener killListener;
    @Mock
    private IRPCClient rpcClient;
    @Mock
    private IRPCClient.IMessageStream<Quit, Session> messageStream;
    private SessionRegister regPipeline;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(rpcClient.createMessageStream(eq(tenantId), any(), eq(registryKey), anyMap(),
            eq(SessionDictServiceGrpc.getDictMethod()))).thenReturn(messageStream);
        regPipeline = new SessionRegister(tenantId, registryKey, rpcClient);
    }

    @AfterMethod
    @SneakyThrows
    public void tearDown() {
        closeable.close();
    }

    @Test
    public void regAfterInit() {
        new SessionRegistration(owner, killListener, regPipeline);
        ArgumentCaptor<Session> argCaptor = ArgumentCaptor.forClass(Session.class);
        verify(messageStream).ack(argCaptor.capture());
        assertEquals(argCaptor.getValue().getOwner(), owner);
        assertTrue(argCaptor.getValue().getKeep());
    }

    @Test
    public void reRegAfterRetarget() {
        new SessionRegistration(owner, killListener, regPipeline);
        ArgumentCaptor<Consumer<Long>> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);

        verify(messageStream).onRetarget(consumerCaptor.capture());
        consumerCaptor.getValue().accept(System.nanoTime());

        ArgumentCaptor<Session> argCaptor = ArgumentCaptor.forClass(Session.class);
        verify(messageStream, times(2)).ack(argCaptor.capture());
        assertEquals(argCaptor.getValue().getOwner(), owner);
        assertTrue(argCaptor.getValue().getKeep());
    }

    @Test
    public void quitAndStop() {
        new SessionRegistration(owner, killListener, regPipeline);
        ArgumentCaptor<Consumer<Quit>> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        Quit quit = Quit.newBuilder()
            .setReqId(System.nanoTime())
            .setOwner(owner)
            .setKiller(ClientInfo.newBuilder().setTenantId(tenantId).setType("MockKiller").build())
            .build();
        verify(messageStream).onMessage(consumerCaptor.capture());
        consumerCaptor.getValue().accept(quit);

        ArgumentCaptor<ClientInfo> killerCaptor = ArgumentCaptor.forClass(ClientInfo.class);
        verify(killListener).onKill(killerCaptor.capture(),
            argThat(r -> r.getType() == ServerRedirection.Type.NO_MOVE));
        assertEquals(killerCaptor.getValue(), quit.getKiller());
    }

    @Test
    public void ignoreQuit() {
        new SessionRegistration(owner, killListener, regPipeline);
        ArgumentCaptor<Consumer<Quit>> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        Quit quit = Quit.newBuilder()
            .setReqId(System.nanoTime())
            .setOwner(owner.toBuilder().putMetadata(MQTT_CLIENT_ID_KEY, "FakeClient").build())
            .setKiller(ClientInfo.newBuilder().setTenantId(tenantId).setType("MockKiller").build())
            .build();
        verify(messageStream).onMessage(consumerCaptor.capture());
        consumerCaptor.getValue().accept(quit);

        verify(killListener, times(0)).onKill(any(), isNull());
        verify(messageStream, times(1)).ack(any());
    }

    @Test
    public void stop() {
        SessionRegistration register = new SessionRegistration(owner, killListener, regPipeline);
        register.stop();
        ArgumentCaptor<Session> sessionCaptor = ArgumentCaptor.forClass(Session.class);
        verify(messageStream, times(2)).ack(sessionCaptor.capture());
        Session session = sessionCaptor.getAllValues().get(1);
        assertEquals(session.getOwner(), owner);
        assertFalse(session.getKeep());
    }

    @Test
    public void registerGC() throws InterruptedException {
        SessionRegistration register =
            new SessionRegistration(owner, killListener, new SessionRegister(tenantId, registryKey, rpcClient));
        register.stop();
        register = null;
        System.gc();
        Thread.sleep(10);
        await().until(() -> {
            verify(messageStream, times(1)).close();
            return true;
        });
    }

    @Test
    public void regPipelineClose() throws InterruptedException {
        SessionRegister sessionRegPipeline = new SessionRegister(tenantId, registryKey, rpcClient);
        sessionRegPipeline.close();
        Thread.sleep(10);
        await().until(() -> {
            verify(messageStream, times(1)).close();
            return true;
        });
    }
}
