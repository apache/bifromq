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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.resourcethrottler.TenantResourceType;

class WebHookBasedResourceThrottler implements IResourceThrottler {
    private final AsyncLoadingCache<ResourceKey, Boolean> resultCache;
    private final HttpClient httpClient;
    WebHookBasedResourceThrottler(URI webhookURI) {
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
        this.resultCache = Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofSeconds(60))
            .refreshAfterWrite(Duration.ofSeconds(1))
            .buildAsync((key, executor) -> {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(webhookURI)
                    .GET()
                    .timeout(Duration.ofSeconds(5))
                    .header("tenant_id", key.tenantId())
                    .header("resource_type", key.type().name())
                    .build();
                return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenApply(response -> {
                        if (response.statusCode() == 200) {
                            return Boolean.parseBoolean(response.body());
                        } else {
                            return true;
                        }
                    })
                    .exceptionally(e -> {
                        System.out.println("Failed to call webhook: " + e.getMessage());
                        return true;
                    });
            });
    }

    public boolean hasResource(String tenantId, TenantResourceType type) {
        CompletableFuture<Boolean>
            resultFuture = resultCache.get(new ResourceKey(tenantId, type));
        if (resultFuture.isDone()) {
            try {
                return resultFuture.join();
            } catch (Throwable e) {
                return false;
            }
        }
        return true;
    }

    private record ResourceKey(String tenantId, TenantResourceType type) {
    }
}
