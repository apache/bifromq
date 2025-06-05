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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CacheableSettingProviderTest {
    @Mock
    private ISettingProvider provider;
    @Mock
    private CacheOptions options;

    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);

        when(options.expireDuration()).thenReturn(Duration.ofSeconds(1));
        when(options.maxCachedTenants()).thenReturn(10);
        when(options.refreshDuration()).thenReturn(Duration.ofSeconds(1));
        when(options.provideInitValue()).thenReturn(false);
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        closeable.close();
    }

    @Test
    public void provideInitialValue() {
        when(options.provideInitValue()).thenReturn(true);
        CacheableSettingProvider cacheableSettingProvider = new CacheableSettingProvider(provider, options);
        String tenantId = "tenantA";
        int newValue = (int) Setting.MaxTopicLevels.initialValue() + 64;
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenReturn(newValue);
        assertEquals((int) cacheableSettingProvider.provide(Setting.MaxTopicLevels, tenantId), newValue);

        int newValue2 = newValue + 64;
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenReturn(newValue2);
        await().until(() -> (int) cacheableSettingProvider.provide(Setting.MaxTopicLevels, tenantId) == newValue2);
    }

    @Test
    public void usingInitialValue() {
        CacheableSettingProvider cacheableSettingProvider = new CacheableSettingProvider(provider, options);
        String tenantId = "tenantA";
        int initValue = Setting.MaxTopicLevels.initialValue();
        int newValue = initValue + 64;
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenReturn(newValue);
        assertEquals((int) cacheableSettingProvider.provide(Setting.MaxTopicLevels, tenantId), initValue);
        await().until(() -> (int) cacheableSettingProvider.provide(Setting.MaxTopicLevels, tenantId) == newValue);
    }

    @Test
    public void provideNullFallbackToInitialValue() {
        when(options.provideInitValue()).thenReturn(true);
        CacheableSettingProvider cacheableSettingProvider = new CacheableSettingProvider(provider, options);
        String tenantId = "tenantA";
        int initValue = Setting.MaxTopicLevels.initialValue();
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenReturn(null);
        assertEquals((int) cacheableSettingProvider.provide(Setting.MaxTopicLevels, tenantId), initValue);
    }

    @SneakyThrows
    @Test
    public void refreshNullFallbackToOldValue() {
        CacheableSettingProvider cacheableSettingProvider = new CacheableSettingProvider(provider, options);
        String tenantId = "tenantA";
        int initValue = Setting.MaxTopicLevels.initialValue();
        assertEquals((int) cacheableSettingProvider.provide(Setting.MaxTopicLevels, tenantId), initValue);

        int newValue = initValue + 64;
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenReturn(newValue);
        await().until(() -> (int) cacheableSettingProvider.provide(Setting.MaxTopicLevels, tenantId) == newValue);

        AtomicInteger counter = new AtomicInteger(0);
        when(provider.provide(Setting.MaxTopicLevels, tenantId)).thenAnswer(invocation -> {
            counter.getAndIncrement();
            return null;
        });
        await().until(() -> (int)
            cacheableSettingProvider.provide(Setting.MaxTopicLevels, tenantId) == newValue && counter.get() > 1);
    }
}
