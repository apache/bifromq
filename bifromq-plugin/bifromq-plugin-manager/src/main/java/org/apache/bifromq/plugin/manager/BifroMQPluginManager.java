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

import lombok.extern.slf4j.Slf4j;
import org.pf4j.CompoundPluginLoader;
import org.pf4j.DefaultPluginManager;
import org.pf4j.ExtensionFactory;
import org.pf4j.ExtensionFinder;
import org.pf4j.PluginFactory;
import org.pf4j.PluginLoader;

@Slf4j
public class BifroMQPluginManager extends DefaultPluginManager implements AutoCloseable {
    public BifroMQPluginManager() {
        loadPlugins();
        startPlugins();
    }

    @Override
    protected PluginLoader createPluginLoader() {
        return new CompoundPluginLoader()
            .add(new BifroMQDevelopmentPluginLoader(this), this::isDevelopment)
            .add(new BifroMQJarPluginLoader(this), this::isNotDevelopment)
            .add(new BifroMQDefaultPluginLoader(this), this::isNotDevelopment);
    }

    @Override
    protected PluginFactory createPluginFactory() {
        return new BifroMQPluginFactory();
    }

    @Override
    protected ExtensionFinder createExtensionFinder() {
        BifroMQExtensionFinder extensionFinder = new BifroMQExtensionFinder(this);
        addPluginStateListener(extensionFinder);
        return extensionFinder;
    }

    @Override
    protected ExtensionFactory createExtensionFactory() {
        return new BifroMQExtensionFactory(this);
    }

    @Override
    public void close() {
        stopPlugins();
        unloadPlugins();
    }
}
