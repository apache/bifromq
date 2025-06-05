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

package org.apache.bifromq.demo.plugin;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.plugin.settingprovider.Setting;

class WebHookBasedSettingProvider implements ISettingProvider {

    private final URI webhookURI;
    private final HttpClient httpClient;

    WebHookBasedSettingProvider(URI webhookURI) {
        this.webhookURI = webhookURI;
        this.httpClient =
            HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> R provide(Setting setting, String tenantId) {
        HttpRequest request =
            HttpRequest.newBuilder().uri(webhookURI).GET().timeout(Duration.ofSeconds(5)).header("tenant_id", tenantId)
                .header("setting_name", setting.name()).build();
        Object val = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply(response -> {
                if (response.statusCode() == 200) {
                    try {
                        if (setting.valueType == Integer.class) {
                            return Integer.parseInt(response.body());
                        }
                        if (setting.valueType == Long.class) {
                            return Long.parseLong(response.body());
                        }
                        if (setting.valueType == Boolean.class) {
                            return Boolean.parseBoolean(response.body());
                        }
                        return null;
                    } catch (Throwable e) {
                        return null;
                    }
                } else {
                    return null;
                }
            })
            .thenApply(r -> setting.isValid(r, tenantId) ? r : null)
            .exceptionally(e -> {
                System.out.println("Failed to call webhook: " + e.getMessage());
                return null;
            })
            .join();
        return (R) val;
    }
}
