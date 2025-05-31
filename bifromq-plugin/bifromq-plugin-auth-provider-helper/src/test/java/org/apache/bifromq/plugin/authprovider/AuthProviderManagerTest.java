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

package org.apache.bifromq.plugin.authprovider;

import static org.apache.bifromq.plugin.authprovider.MetricConstants.CALL_FAIL_COUNTER;
import static org.apache.bifromq.plugin.authprovider.MetricConstants.CALL_TIMER;
import static org.apache.bifromq.plugin.authprovider.MetricConstants.TAG_METHOD;
import static org.apache.bifromq.plugin.settingprovider.Setting.ByPassPermCheckError;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Error;
import org.apache.bifromq.plugin.authprovider.type.Failed;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.PubAction;
import org.apache.bifromq.plugin.authprovider.type.Reject;
import org.apache.bifromq.plugin.authprovider.type.SubAction;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.mqttbroker.accessctrl.AccessControlError;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.type.ClientInfo;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class AuthProviderManagerTest {
    @Mock
    PluginManager pluginManager;
    @Mock
    private IAuthProvider mockProvider;
    @Mock
    private ISettingProvider settingProvider;
    @Mock
    private IEventCollector eventCollector;
    private MeterRegistry meterRegistry;
    private ClientInfo clientInfo = ClientInfo.getDefaultInstance();
    private MQTT3AuthData mockAuth3Data = MQTT3AuthData.newBuilder().build();
    private MQTT5AuthData mockAuth5Data = MQTT5AuthData.newBuilder().build();
    private MQTT5ExtendedAuthData mockExtAuth5Data = MQTT5ExtendedAuthData.newBuilder().build();
    private MQTTAction mockActionInfo = MQTTAction.newBuilder().setPub(PubAction.getDefaultInstance()).build();
    private AuthProviderManager manager;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
        closeable = MockitoAnnotations.openMocks(this);
        clientInfo = ClientInfo.getDefaultInstance();
        mockAuth3Data = MQTT3AuthData.newBuilder().build();
        mockActionInfo = MQTTAction.newBuilder().setPub(PubAction.getDefaultInstance()).build();
        when(pluginManager.getExtensions(IAuthProvider.class)).thenReturn(
            Collections.singletonList(mockProvider));
    }

    @AfterMethod
    public void tearDown() throws Exception {
        closeable.close();
        meterRegistry.clear();
        Metrics.globalRegistry.clear();
    }

    @Test
    public void devOnlyMode() {
        when(pluginManager.getExtensions(IAuthProvider.class)).thenReturn(Collections.emptyList());
        manager = new AuthProviderManager(null, pluginManager, settingProvider, eventCollector);
        MQTT3AuthResult result = manager.auth(mockAuth3Data).join();
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.OK);
        assertEquals(result.getOk().getTenantId(), "DevOnly");

        boolean allow = manager.check(ClientInfo.getDefaultInstance(), MQTTAction.newBuilder()
            .setSub(SubAction.getDefaultInstance()).build()).join();
        assertTrue(allow);
        allow = manager.check(ClientInfo.getDefaultInstance(), MQTTAction.newBuilder()
            .setSub(SubAction.getDefaultInstance()).build()).join();
        assertTrue(allow);
        manager.close();
    }

    @Test
    public void pluginSpecified() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(mockAuth3Data)).thenReturn(CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
            .setReject(Reject.newBuilder()
                .setCode(Reject.Code.BadPass)
                .build()).build()));
        MQTT3AuthResult result = manager.auth(mockAuth3Data).join();
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.REJECT);
        assertEquals(result.getReject().getCode(), Reject.Code.BadPass);
        manager.close();
    }

    @Test
    public void pluginNotFound() {
        manager = new AuthProviderManager("Fake", pluginManager, settingProvider, eventCollector);
        MQTT3AuthResult result = manager.auth(mockAuth3Data).join();
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.OK);
        assertEquals(result.getOk().getTenantId(), "DevOnly");

        boolean allow = manager.check(ClientInfo.getDefaultInstance(), MQTTAction.newBuilder()
            .setSub(SubAction.getDefaultInstance()).build()).join();
        assertTrue(allow);
        allow = manager.check(ClientInfo.getDefaultInstance(), MQTTAction.newBuilder()
            .setSub(SubAction.getDefaultInstance()).build()).join();
        assertTrue(allow);
        manager.close();
    }

    @Test
    public void auth3OK() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
                return MQTT3AuthResult.newBuilder()
                    .setReject(Reject.newBuilder()
                        .setCode(Reject.Code.BadPass)
                        .build())
                    .build();
            }));
        MQTT3AuthResult result = manager.auth(mockAuth3Data).join();
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.REJECT);
        assertEquals(result.getReject().getCode(), Reject.Code.BadPass);
        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/auth").timer().count(),
            1);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/auth").counter().count(),
            0);
        manager.close();
    }

    @Test
    public void auth3ReturnError() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT3AuthData.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        MQTT3AuthResult result = manager.auth(mockAuth3Data).join();
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.REJECT);
        assertEquals(result.getReject().getCode(), Reject.Code.Error);
        assertEquals(result.getReject().getReason(), "Intend Error");
        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/auth").timer().count(),
            0);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/auth").counter().count(),
            1);
        manager.close();
    }

    @Test
    public void auth3ThrowsException() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT3AuthData.class)))
            .thenThrow(new RuntimeException("Intend Error"));
        MQTT3AuthResult result = manager.auth(mockAuth3Data).join();
        assertEquals(result.getTypeCase(), MQTT3AuthResult.TypeCase.REJECT);
        assertEquals(result.getReject().getCode(), Reject.Code.Error);
        assertEquals(result.getReject().getReason(), "Intend Error");
        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/auth").timer().count(),
            0);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/auth").counter().count(),
            1);
        manager.close();
    }

    @Test
    public void auth5OK() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
                return MQTT5AuthResult.newBuilder()
                    .setFailed(Failed.newBuilder()
                        .setCode(Failed.Code.BadPass)
                        .build())
                    .build();
            }));
        MQTT5AuthResult result = manager.auth(mockAuth5Data).join();
        assertEquals(result.getTypeCase(), MQTT5AuthResult.TypeCase.FAILED);
        assertEquals(result.getFailed().getCode(), Failed.Code.BadPass);
        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/auth").timer().count(),
            1);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/auth").counter().count(),
            0);
        manager.close();
    }

    @Test
    public void auth5ReturnError() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT5AuthData.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        MQTT5AuthResult result = manager.auth(mockAuth5Data).join();
        assertEquals(result.getTypeCase(), MQTT5AuthResult.TypeCase.FAILED);
        assertEquals(result.getFailed().getCode(), Failed.Code.Error);
        assertEquals(result.getFailed().getReason(), "Intend Error");
        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/auth").timer().count(),
            0);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/auth").counter().count(),
            1);
        manager.close();
    }

    @Test
    public void auth5ThrowsException() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.auth(any(MQTT5AuthData.class)))
            .thenThrow(new RuntimeException("Intend Error"));
        MQTT5AuthResult result = manager.auth(mockAuth5Data).join();
        assertEquals(result.getTypeCase(), MQTT5AuthResult.TypeCase.FAILED);
        assertEquals(result.getFailed().getCode(), Failed.Code.Error);
        assertEquals(result.getFailed().getReason(), "Intend Error");
        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/auth").timer().count(),
            0);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/auth").counter().count(),
            1);
        manager.close();
    }

    @Test
    public void extAuth5OK() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenReturn(CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // ignore
                }
                return MQTT5ExtendedAuthResult.newBuilder()
                    .setFailed(Failed.newBuilder()
                        .setCode(Failed.Code.BadPass)
                        .build())
                    .build();
            }));
        MQTT5ExtendedAuthResult result = manager.extendedAuth(mockExtAuth5Data).join();
        assertEquals(result.getTypeCase(), MQTT5ExtendedAuthResult.TypeCase.FAILED);
        assertEquals(result.getFailed().getCode(), Failed.Code.BadPass);
        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/extAuth").timer().count(),
            1);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/extAuth").counter().count(),
            0);
        manager.close();
    }

    @Test
    public void extAuth5ReturnError() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        MQTT5ExtendedAuthResult result = manager.extendedAuth(mockExtAuth5Data).join();
        assertEquals(result.getTypeCase(), MQTT5ExtendedAuthResult.TypeCase.FAILED);
        assertEquals(result.getFailed().getCode(), Failed.Code.Error);
        assertEquals(result.getFailed().getReason(), "Intend Error");
        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/extAuth").timer().count(),
            0);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/extAuth").counter().count(),
            1);
        manager.close();
    }

    @Test
    public void extAuth5ThrowsException() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.extendedAuth(any(MQTT5ExtendedAuthData.class)))
            .thenThrow(new RuntimeException("Intend Error"));
        MQTT5ExtendedAuthResult result = manager.extendedAuth(mockExtAuth5Data).join();
        assertEquals(result.getTypeCase(), MQTT5ExtendedAuthResult.TypeCase.FAILED);
        assertEquals(result.getFailed().getCode(), Failed.Code.Error);
        assertEquals(result.getFailed().getReason(), "Intend Error");
        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/extAuth").timer().count(),
            0);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/extAuth").counter().count(),
            1);
        manager.close();
    }

    @Test
    public void checkPermissionReturnGranted() {
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.checkPermission(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.completedFuture(
                CheckResult.newBuilder().setGranted(Granted.getDefaultInstance()).build()));
        CheckResult result = manager.checkPermission(clientInfo, mockActionInfo).join();
        assertTrue(result.hasGranted());

        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/check").timer().count(),
            1);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/check").counter().count(),
            0);
        manager.close();
    }

    @Test
    public void checkPermissionReturnErrorAndNoPass() {
        when(settingProvider.provide(ByPassPermCheckError, clientInfo.getTenantId())).thenReturn(false);
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.checkPermission(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        assertFalse(manager.checkPermission(clientInfo, mockActionInfo).join().hasDenied());
        ArgumentCaptor<AccessControlError> eventArgumentCaptor = ArgumentCaptor.forClass(AccessControlError.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        assertEquals(eventArgumentCaptor.getValue().type(), EventType.ACCESS_CONTROL_ERROR);
        assertTrue(eventArgumentCaptor.getValue().cause().contains("Intend Error"));

        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/check").timer().count(),
            0);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/check").counter().count(),
            1);
    }

    @Test
    public void checkPermissionThrowsExceptionAndPass() {
        when(settingProvider.provide(ByPassPermCheckError, clientInfo.getTenantId())).thenReturn(true);
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.checkPermission(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Intend Error")));
        assertTrue(manager.checkPermission(clientInfo, mockActionInfo).join().hasGranted());
        ArgumentCaptor<AccessControlError> eventArgumentCaptor = ArgumentCaptor.forClass(AccessControlError.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        assertEquals(eventArgumentCaptor.getValue().type(), EventType.ACCESS_CONTROL_ERROR);

        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/check").timer().count(),
            0);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/check").counter().count(),
            1);
    }

    @Test
    public void byPassCheckResultError() {
        when(settingProvider.provide(ByPassPermCheckError, clientInfo.getTenantId())).thenReturn(true);
        manager =
            new AuthProviderManager(mockProvider.getClass().getName(), pluginManager, settingProvider, eventCollector);
        when(mockProvider.checkPermission(any(ClientInfo.class), any(MQTTAction.class)))
            .thenReturn(CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setError(Error.newBuilder().build())
                .build()));
        assertTrue(manager.checkPermission(clientInfo, mockActionInfo).join().hasGranted());
        ArgumentCaptor<AccessControlError> eventArgumentCaptor = ArgumentCaptor.forClass(AccessControlError.class);
        verify(eventCollector).report(eventArgumentCaptor.capture());
        assertEquals(eventArgumentCaptor.getValue().type(), EventType.ACCESS_CONTROL_ERROR);

        assertEquals(meterRegistry.find(CALL_TIMER).tag(TAG_METHOD, "AuthProvider/check").timer().count(),
            1);
        assertEquals(meterRegistry.find(CALL_FAIL_COUNTER).tag(TAG_METHOD, "AuthProvider/check").counter().count(),
            0);
    }
}
