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

package org.apache.bifromq.plugin.manager;

import static org.pf4j.RuntimeMode.DEVELOPMENT;

import org.apache.bifromq.plugin.BifroMQPlugin;
import org.apache.bifromq.plugin.BifroMQPluginDescriptor;
import java.lang.reflect.Constructor;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.DefaultPluginFactory;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;

@Slf4j
public class BifroMQPluginFactory extends DefaultPluginFactory {
    protected Plugin createInstance(Class<?> pluginClass, PluginWrapper pluginWrapper) {
        ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(pluginWrapper.getPluginClassLoader());
            if (BifroMQPlugin.class.isAssignableFrom(pluginClass)) {
                return createBifroMQPluginInstance(pluginClass, pluginWrapper);
            }
            return super.createInstance(pluginClass, pluginWrapper);
        } finally {
            Thread.currentThread().setContextClassLoader(originalLoader);
        }
    }

    private Plugin createBifroMQPluginInstance(Class<?> pluginClass, PluginWrapper pluginWrapper) {
        BifroMQPluginDescriptor context = new BifroMQPluginDescriptor(
            pluginWrapper.getDescriptor(),
            pluginWrapper.getPluginPath(),
            pluginWrapper.getPluginClassLoader(),
            pluginWrapper.getRuntimeMode() == DEVELOPMENT);
        try {
            Constructor<?> constructor = pluginClass.getDeclaredConstructor(BifroMQPluginDescriptor.class);
            return (Plugin) constructor.newInstance(context);
        } catch (Exception e) {
            log.error("Failed to initialize BifroMQ Plugin[{}]", pluginClass.getName(), e);
        }
        return null;
    }
}
