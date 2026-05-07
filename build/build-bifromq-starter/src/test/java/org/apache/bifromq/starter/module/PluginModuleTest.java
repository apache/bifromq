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

package org.apache.bifromq.starter.module;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scope;
import com.google.inject.spi.BindingScopingVisitor;
import com.google.inject.spi.Elements;
import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.mqtt.inbox.IMqttBrokerClient;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.SettingProviderManager;
import org.apache.bifromq.starter.config.StandaloneConfig;
import org.pf4j.PluginManager;
import org.testng.annotations.Test;

public class PluginModuleTest {
    @Test
    public void pluginRuntimeBindsPluginManagerAsEagerSingleton() {
        AtomicBoolean eagerSingleton = new AtomicBoolean();
        Binding<?> pluginManagerBinding = Elements.getElements(new PluginRuntimeModule()).stream()
            .filter(Binding.class::isInstance)
            .map(Binding.class::cast)
            .filter(binding -> binding.getKey().getTypeLiteral().getRawType().equals(PluginManager.class))
            .findFirst()
            .orElse(null);

        assertNotNull(pluginManagerBinding);
        pluginManagerBinding.acceptScopingVisitor(new BindingScopingVisitor<Void>() {
            @Override
            public Void visitEagerSingleton() {
                eagerSingleton.set(true);
                return null;
            }

            @Override
            public Void visitScope(Scope scope) {
                return null;
            }

            @Override
            public Void visitScopeAnnotation(Class<? extends Annotation> scopeAnnotation) {
                return null;
            }

            @Override
            public Void visitNoScoping() {
                return null;
            }
        });

        assertTrue(eagerSingleton.get());
    }

    @Test
    public void pluginExtensionManagersAreLazy() {
        PluginManager pluginManager = mock(PluginManager.class);

        Guice.createInjector(
            binder -> {
                binder.bind(StandaloneConfig.class).toInstance(new StandaloneConfig());
                binder.bind(PluginManager.class).toInstance(pluginManager);
                binder.bind(SharedResourcesHolder.class).toInstance(sharedResourcesHolder());
                binder.bind(IMqttBrokerClient.class).toInstance(mock(IMqttBrokerClient.class));
                binder.bind(IInboxClient.class).toInstance(mock(IInboxClient.class));
            },
            new PluginExtensionModule());

        verifyNoInteractions(pluginManager);
    }

    @Test
    public void settingProviderLoadsExtensionWhenRequested() {
        PluginManager pluginManager = mock(PluginManager.class);
        when(pluginManager.getExtensions(ISettingProvider.class)).thenReturn(Collections.emptyList());
        Injector injector = Guice.createInjector(
            binder -> {
                binder.bind(StandaloneConfig.class).toInstance(new StandaloneConfig());
                binder.bind(PluginManager.class).toInstance(pluginManager);
                binder.bind(SharedResourcesHolder.class).toInstance(sharedResourcesHolder());
                binder.bind(IMqttBrokerClient.class).toInstance(mock(IMqttBrokerClient.class));
                binder.bind(IInboxClient.class).toInstance(mock(IInboxClient.class));
            },
            new PluginExtensionModule());

        injector.getInstance(SettingProviderManager.class);

        verify(pluginManager).getExtensions(ISettingProvider.class);
    }

    private SharedResourcesHolder sharedResourcesHolder() {
        SharedResourcesHolder holder = mock(SharedResourcesHolder.class);
        when(holder.add(any())).thenAnswer(invocation -> invocation.getArgument(0));
        return holder;
    }
}
