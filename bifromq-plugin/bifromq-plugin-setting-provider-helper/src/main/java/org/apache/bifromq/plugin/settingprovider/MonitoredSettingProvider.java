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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class MonitoredSettingProvider implements ISettingProvider {
    private static final Logger pluginLog = LoggerFactory.getLogger("plugin.manager");
    private final Timer provideCallTimer;
    private final Counter provideCallErrorCounter;
    private final ISettingProvider delegate;

    public MonitoredSettingProvider(ISettingProvider delegate) {
        this.delegate = delegate;
        provideCallTimer = Timer.builder("call.exec.timer")
            .tag("method", "SettingProvider/provide")
            .tag("type", delegate.getClass().getName())
            .register(Metrics.globalRegistry);
        provideCallErrorCounter = Counter.builder("call.exec.fail.count")
            .tag("method", "SettingProvider/provide")
            .tag("type", delegate.getClass().getName())
            .register(Metrics.globalRegistry);
    }

    @Override
    public <R> R provide(Setting setting, String tenantId) {
        try {
            Timer.Sample sample = Timer.start();
            R newVal = delegate.provide(setting, tenantId);
            sample.stop(provideCallTimer);
            if (newVal == null || setting.isValid(newVal, tenantId)) {
                return newVal;
            } else {
                pluginLog.warn("Invalid setting value: setting={}, value={}", setting.name(), newVal);
                provideCallErrorCounter.increment();
                return null;
            }
        } catch (Throwable e) {
            pluginLog.error("Setting provider throws exception: setting={}", setting.name(), e);
            provideCallErrorCounter.increment();
            return null;
        }
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } catch (Throwable e) {
            pluginLog.error("Setting provider close throws exception", e);
        }
        Metrics.globalRegistry.remove(provideCallTimer);
        Metrics.globalRegistry.remove(provideCallErrorCounter);
    }
}
