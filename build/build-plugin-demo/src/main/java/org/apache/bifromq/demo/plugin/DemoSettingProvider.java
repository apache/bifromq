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

package org.apache.bifromq.demo.plugin;

import java.net.URI;
import java.util.EnumMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;
import org.pf4j.Extension;

@Slf4j
@Extension
public class DemoSettingProvider implements ISettingProvider {
    private static final String PLUGIN_RESOURCE_THROTTLER_URL = "plugin.settingprovider.url";
    private final EnumMap<Setting, Object> initialValues = new EnumMap<>(Setting.class);
    private final ISettingProvider delegate;

    public DemoSettingProvider() {
        ISettingProvider delegate1;
        String webhookUrl = System.getProperty(PLUGIN_RESOURCE_THROTTLER_URL);
        for (Setting setting : Setting.values()) {
            initialValues.put(setting, setting.current("DevOnly"));
        }
        if (webhookUrl == null) {
            log.info("No webhook url specified, fallback to dev only setting provider.");
            delegate1 = new ISettingProvider() {
                @Override
                public <R> R provide(Setting setting, String tenantId) {
                    return (R) initialValues.get(setting);
                }
            };
        } else {
            try {
                URI webhookURI = URI.create(webhookUrl);
                delegate1 = new WebHookBasedSettingProvider(webhookURI);
                log.info("DemoSettingProvider's webhook URL: {}", webhookUrl);
            } catch (Throwable e) {
                delegate1 = new ISettingProvider() {
                    @Override
                    public <R> R provide(Setting setting, String tenantId) {
                        return (R) initialValues.get(setting);
                    }
                };
            }
        }
        delegate = delegate1;

    }

    @Override
    public <R> R provide(Setting setting, String tenantId) {
        return delegate.provide(setting, tenantId);
    }
}
