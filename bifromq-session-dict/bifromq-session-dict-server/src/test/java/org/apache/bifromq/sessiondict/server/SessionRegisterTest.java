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

package org.apache.bifromq.sessiondict.server;

import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_TYPE_VALUE;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.baserpc.RPCContext;
import org.apache.bifromq.baserpc.metrics.IRPCMeter;
import org.apache.bifromq.baserpc.metrics.RPCMetric;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import io.micrometer.core.instrument.Timer;
import java.util.Collections;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.bifromq.sessiondict.rpc.proto.Quit;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.sessiondict.rpc.proto.Session;
import org.apache.bifromq.type.ClientInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SessionRegisterTest {
    private final String tenantId = "tenantId";
    private final String userId = "userId";
    private final String clientId = "clientId";
    private final ClientInfo owner = ClientInfo.newBuilder()
        .setTenantId(tenantId)
        .setType(MQTT_TYPE_VALUE)
        .putMetadata(MQTT_USER_ID_KEY, userId)
        .putMetadata(MQTT_CLIENT_ID_KEY, clientId)
        .putMetadata(MQTT_CHANNEL_ID_KEY, UUID.randomUUID().toString())
        .build();
    private final ClientInfo killer = ClientInfo.newBuilder().setTenantId(tenantId).setType("killer").build();
    private AutoCloseable closeable;
    @Mock
    private ISessionRegister.IRegistrationListener listener;

    @Mock
    private ServerCallStreamObserver<Quit> responseObserver;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterMethod
    @SneakyThrows
    public void tearDown() {
        closeable.close();
    }

    @Test
    public void reg() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            ArgumentCaptor<ClientInfo> ownerCaptor = ArgumentCaptor.forClass(ClientInfo.class);
            ArgumentCaptor<Boolean> keepCaptor = ArgumentCaptor.forClass(Boolean.class);
            ArgumentCaptor<ISessionRegister> registerCaptor = ArgumentCaptor.forClass(ISessionRegister.class);
            verify(listener).on(ownerCaptor.capture(), keepCaptor.capture(), registerCaptor.capture());
            assertEquals(ownerCaptor.getValue(), owner);
            assertTrue(keepCaptor.getValue());
            assertEquals(registerCaptor.getValue(), register);
        });
    }

    @Test
    public void unreg() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(false)
                .build());
            ArgumentCaptor<ClientInfo> ownerCaptor = ArgumentCaptor.forClass(ClientInfo.class);
            ArgumentCaptor<Boolean> keepCaptor = ArgumentCaptor.forClass(Boolean.class);
            ArgumentCaptor<ISessionRegister> registerCaptor = ArgumentCaptor.forClass(ISessionRegister.class);
            verify(listener, times(2))
                .on(ownerCaptor.capture(), keepCaptor.capture(), registerCaptor.capture());
            assertEquals(ownerCaptor.getAllValues().get(1), owner);
            assertFalse(keepCaptor.getAllValues().get(1));
            assertEquals(registerCaptor.getAllValues().get(1), register);
        });
    }

    @Test
    public void kick() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            register.onNext(Session.newBuilder()
                .setReqId(System.nanoTime())
                .setOwner(owner)
                .setKeep(true)
                .build());
            ServerRedirection redirection =
                ServerRedirection.newBuilder().setType(ServerRedirection.Type.PERMANENT_MOVE).build();
            register.kick(tenantId, owner, killer, redirection);
            verify(responseObserver).onNext(argThat(quit -> quit.getOwner().equals(owner)
                && quit.getKiller().equals(killer)
                && quit.getServerRedirection().equals(redirection)));
            verify(listener).on(eq(owner), eq(false), eq(register));
        });
    }

    @Test
    public void kickNonExist() {
        test(() -> {
            SessionRegister register = new SessionRegister(listener, responseObserver);
            ServerRedirection redirection =
                ServerRedirection.newBuilder().setType(ServerRedirection.Type.PERMANENT_MOVE).build();
            register.kick(tenantId, owner, killer, redirection);
            verify(responseObserver, never()).onNext(any());
            verify(listener, never()).on(any(), anyBoolean(), any());
        });
    }


    private void test(Runnable runnable) {
        Context ctx = Context.ROOT
            .withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId)
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, Collections.emptyMap())
            .withValue(RPCContext.METER_KEY_CTX_KEY, new IRPCMeter.IRPCMethodMeter() {
                @Override
                public void recordCount(RPCMetric metric) {

                }

                @Override
                public void recordCount(RPCMetric metric, double inc) {

                }

                @Override
                public Timer timer(RPCMetric metric) {
                    return null;
                }

                @Override
                public void recordSummary(RPCMetric metric, int depth) {

                }
            });
        ctx.run(runnable);
    }
}
