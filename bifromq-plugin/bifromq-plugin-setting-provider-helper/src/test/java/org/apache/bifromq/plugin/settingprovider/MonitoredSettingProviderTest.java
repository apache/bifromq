/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.plugin.settingprovider;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MonitoredSettingProviderTest {
    @Mock
    private ISettingProvider provider;

    private SimpleMeterRegistry registry;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        registry = new SimpleMeterRegistry();
        Metrics.globalRegistry.clear();
        Metrics.addRegistry(registry);
        closeable = MockitoAnnotations.openMocks(this);
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        Metrics.removeRegistry(registry);
        closeable.close();
    }

    @Test
    public void provideValidValue() {
        MonitoredSettingProvider monitoredSettingProvider = new MonitoredSettingProvider(provider);
        String tenantId = "tenantA";
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenReturn(64);
        int levels = monitoredSettingProvider.provide(Setting.MaxTopicLevels, tenantId);
        assertEquals(levels, 64);
        verify(provider).provide(Setting.MaxTopicLevels, tenantId);
        assertEquals(registry.get("call.exec.timer").timer().count(), 1);
    }

    @Test
    public void provideInvalidValue() {
        MonitoredSettingProvider monitoredSettingProvider = new MonitoredSettingProvider(provider);
        String tenantId = "tenantA";
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenReturn(new Object());
        assertNull(monitoredSettingProvider.provide(Setting.MaxTopicLevels, tenantId));
        verify(provider).provide(Setting.MaxTopicLevels, tenantId);
        assertEquals(registry.get("call.exec.timer").timer().count(), 1);
        assertEquals(registry.get("call.exec.fail.count").counter().count(), 1);
    }

    @Test
    public void provideNullValue() {
        MonitoredSettingProvider monitoredSettingProvider = new MonitoredSettingProvider(provider);
        String tenantId = "tenantA";
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenReturn(null);
        assertNull(monitoredSettingProvider.provide(Setting.MaxTopicLevels, tenantId));
        verify(provider).provide(Setting.MaxTopicLevels, tenantId);
        assertEquals(registry.get("call.exec.timer").timer().count(), 1);
        assertEquals(registry.get("call.exec.fail.count").counter().count(), 0);
    }

    @Test
    public void provideThrows() {
        MonitoredSettingProvider monitoredSettingProvider = new MonitoredSettingProvider(provider);
        String tenantId = "tenantA";
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenThrow(new RuntimeException("Mocked exception"));
        assertNull(monitoredSettingProvider.provide(Setting.MaxTopicLevels, tenantId));
        verify(provider).provide(Setting.MaxTopicLevels, tenantId);
        assertEquals(registry.get("call.exec.fail.count").counter().count(), 1);
    }
}
