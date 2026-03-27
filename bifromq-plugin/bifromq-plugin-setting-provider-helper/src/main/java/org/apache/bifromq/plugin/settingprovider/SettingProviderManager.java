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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class SettingProviderManager implements ISettingProvider, AutoCloseable {
    private static final Logger pluginLog = LoggerFactory.getLogger("plugin.manager");
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ISettingProvider provider;

    public SettingProviderManager(String settingProviderFQN, PluginManager pluginMgr) {
        Map<String, ISettingProvider> availSettingProviders = pluginMgr.getExtensions(ISettingProvider.class).stream()
            .collect(Collectors.toMap(e -> e.getClass().getName(), e -> e,
                    (k,v) -> v, TreeMap::new));
        if (availSettingProviders.isEmpty()) {
            pluginLog.warn("No setting provider plugin available, use DEV ONLY one instead");


            provider = new MonitoredSettingProvider(new DevOnlySettingProvider());
        } else {
            if (settingProviderFQN == null) {
                if (availSettingProviders.size() > 1) {
                    pluginLog.info("Setting provider plugin type not specified, use the first found");
                }
                String firstSettingProviderFQN = availSettingProviders.keySet().iterator().next();
                pluginLog.info("Setting provider plugin loaded: {}", firstSettingProviderFQN);
                provider = new CacheableSettingProvider(
                        new MonitoredSettingProvider(availSettingProviders.get(firstSettingProviderFQN)),
                        CacheOptions.DEFAULT);
            } else if (!availSettingProviders.containsKey(settingProviderFQN)) {
                pluginLog.warn("Setting provider plugin type '{}' not found, so the system will shut down.",
                    settingProviderFQN);
                throw new SettingProviderException("Setting provider plugin type '%s' not found, so the system will shut down.",
                        settingProviderFQN);
            } else {
                pluginLog.info("Setting provider plugin type: {}", settingProviderFQN);
                provider = new CacheableSettingProvider(
                    new MonitoredSettingProvider(availSettingProviders.get(settingProviderFQN)), CacheOptions.DEFAULT);
            }
        }
        for (Setting setting : Setting.values()) {
            setting.setProvider(provider);
        }
    }

    public <R> R provide(Setting setting, String tenantId) {
        assert !stopped.get();
        return provider.provide(setting, tenantId);
    }

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.debug("Closing setting provider manager");
            provider.close();
            log.debug("Setting provider manager closed");
        }
    }
}
