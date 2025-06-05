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

package org.apache.bifromq.plugin.manager;

import org.apache.bifromq.plugin.BifroMQPlugin;
import org.apache.bifromq.plugin.BifroMQPluginContext;
import java.lang.reflect.Constructor;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.DefaultExtensionFactory;
import org.pf4j.Plugin;
import org.pf4j.PluginManager;
import org.pf4j.PluginRuntimeException;

@Slf4j
public class BifroMQExtensionFactory extends DefaultExtensionFactory {
    private final PluginManager pluginManager;

    public BifroMQExtensionFactory(PluginManager pluginManager) {
        this.pluginManager = pluginManager;
    }

    public <T> T create(Class<T> extensionClass) {
        ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(extensionClass.getClassLoader());
            Plugin ownerPlugin = pluginManager.whichPlugin(extensionClass).getPlugin();
            if (ownerPlugin instanceof BifroMQPlugin) {
                return createExtensionWithContext(extensionClass,
                    (BifroMQPlugin<? extends BifroMQPluginContext>) ownerPlugin);
            }
            return super.create(extensionClass);
        } finally {
            Thread.currentThread().setContextClassLoader(originalLoader);
        }
    }

    private <T, C extends BifroMQPluginContext> T createExtensionWithContext(Class<T> extensionClass,
                                                                             BifroMQPlugin<C> ownerPlugin) {
        try {
            C pluginContext = ownerPlugin.context();
            Constructor<T> constructor = extensionClass.getConstructor(pluginContext.getClass());
            return constructor.newInstance(pluginContext);
        } catch (NoSuchMethodException e) {
            log.debug("No constructor with plugin context found for extension[{}], fallback to no-arg constructor",
                extensionClass.getName());
            return super.create(extensionClass);
        } catch (Throwable e) {
            throw new PluginRuntimeException(e);
        }
    }
}
