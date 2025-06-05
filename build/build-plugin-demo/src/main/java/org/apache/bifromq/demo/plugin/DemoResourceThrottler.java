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
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.resourcethrottler.TenantResourceType;
import org.pf4j.Extension;

@Slf4j
@Extension
public class DemoResourceThrottler implements IResourceThrottler {
    private static final String PLUGIN_RESOURCE_THROTTLER_URL = "plugin.resourcethrottler.url";
    private final IResourceThrottler delegate;

    public DemoResourceThrottler() {
        IResourceThrottler delegate1;
        String webhookUrl = System.getProperty(PLUGIN_RESOURCE_THROTTLER_URL);
        if (webhookUrl == null) {
            log.info("No webhook url specified, fallback to no resource will be throttled.");
            delegate1 = (tenantId, type) -> true;
        } else {
            try {
                URI webhookURI = URI.create(webhookUrl);
                delegate1 = new WebHookBasedResourceThrottler(webhookURI);
                log.info("DemoResourceThrottler's webhook URL: {}", webhookUrl);
            } catch (Throwable e) {
                delegate1 = (tenantId, type) -> true;
            }
        }
        delegate = delegate1;
    }

    @Override

    public boolean hasResource(String tenantId, TenantResourceType type) {
        return delegate.hasResource(tenantId, type);
    }
}
