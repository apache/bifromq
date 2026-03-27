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

package org.apache.bifromq.plugin.settingprovider;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pf4j.PluginManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class SettingProviderManagerTest {
    private static final String TENANT_ID = "tenantA";
    @Mock
    private PluginManager pluginManager;
    @Mock
    private ISettingProvider mockProvider;
    private SettingProviderManager manager;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        // to speed up tests
        System.setProperty(CacheOptions.SettingCacheOptions.SYS_PROP_SETTING_REFRESH_SECONDS, "1");
        closeable = MockitoAnnotations.openMocks(this);
        when(pluginManager.getExtensions(ISettingProvider.class)).thenReturn(
            Collections.singletonList(mockProvider));
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (manager != null) {
            manager.close();
        }
        closeable.close();
        System.clearProperty(CacheOptions.SettingCacheOptions.SYS_PROP_SETTING_REFRESH_SECONDS);
    }

    @Test
    public void devOnlyMode() {
        when(pluginManager.getExtensions(ISettingProvider.class)).thenReturn(Collections.emptyList());
        manager = new SettingProviderManager(null, pluginManager);
        for (Setting setting : Setting.values()) {
            assertEquals(manager.provide(setting, TENANT_ID), (Object) setting.initialValue());
        }
    }

    @Test
    public void pluginSpecified() {
        when(mockProvider.provide(Setting.MaxTopicLevels, TENANT_ID)).thenReturn(64);
        manager = new SettingProviderManager(mockProvider.getClass().getName(), pluginManager);
        await().until(() -> (int) manager.provide(Setting.MaxTopicLevels, TENANT_ID) == 64);
    }

    @Test
    public void pluginNotSpecifiedWithSingleProvider() {
        when(mockProvider.provide(Setting.MaxTopicLevels, TENANT_ID)).thenReturn(32);
        manager = new SettingProviderManager(null, pluginManager);
        await().until(() -> (int) manager.provide(Setting.MaxTopicLevels, TENANT_ID) == 32);
    }

    @Test
    public void pluginNotSpecifiedWithMultipleProviders() {
        ISettingProvider provider1 = new FirstTestSettingProvider();
        ISettingProvider provider2 = new SecondTestSettingProvider();

        when(pluginManager.getExtensions(ISettingProvider.class)).thenReturn(
            Arrays.asList(provider1, provider2));
        manager = new SettingProviderManager(null, pluginManager);

        await().until(() -> (int) manager.provide(Setting.MaxTopicLevels, TENANT_ID) == 100);
    }

    @Test
    public void pluginNotSpecifiedWithMultipleSortByKeyProviders() {
        ISettingProvider provider1 = new FirstTestSettingProvider();
        ISettingProvider provider2 = new SecondTestSettingProvider();

        when(pluginManager.getExtensions(ISettingProvider.class)).thenReturn(
            Arrays.asList(provider2, provider1));
        manager = new SettingProviderManager(null, pluginManager);

        await().until(() -> (int) manager.provide(Setting.MaxTopicLevels, TENANT_ID) == 100);
    }

    @Test(expectedExceptions = SettingProviderException.class)
    public void pluginNotFound() {
        manager = new SettingProviderManager("Fake", pluginManager);
        manager.close();
    }

    @Test
    public void stop() {
        manager = new SettingProviderManager(mockProvider.getClass().getName(), pluginManager);
        manager.close();
        manager = null;
    }

    static class FirstTestSettingProvider implements ISettingProvider {
        @SuppressWarnings("unchecked")
        @Override
        public <R> R provide(Setting setting, String tenantId) {
            if (setting == Setting.MaxTopicLevels) {
                return (R) Integer.valueOf(100);
            }
            return setting.initialValue();
        }
    }

    static class SecondTestSettingProvider implements ISettingProvider {
        @SuppressWarnings("unchecked")
        @Override
        public <R> R provide(Setting setting, String tenantId) {
            if (setting == Setting.MaxTopicLevels) {
                return (R) Integer.valueOf(200);
            }
            return setting.initialValue();
        }
    }
}
