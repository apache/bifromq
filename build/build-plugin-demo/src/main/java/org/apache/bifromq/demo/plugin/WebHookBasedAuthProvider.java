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

import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_USER_ID_KEY;

import com.google.protobuf.util.JsonFormat;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.Reject;
import org.apache.bifromq.type.ClientInfo;

class WebHookBasedAuthProvider implements IAuthProvider {
    private final URI webhookURI;
    private final HttpClient httpClient;

    WebHookBasedAuthProvider(URI webhookURI) {
        this.webhookURI = webhookURI;
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    }

    @Override
    public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(webhookURI + "/auth"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JsonFormat.printer().print(authData)))
                .timeout(Duration.ofSeconds(5))
                .build();
            return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() == 200) {
                        try {
                            MQTT3AuthResult.Builder resultBuilder = MQTT3AuthResult.newBuilder();
                            JsonFormat.parser()
                                .ignoringUnknownFields()
                                .merge(response.body(), resultBuilder);
                            return resultBuilder.build();
                        } catch (Throwable e) {
                            return MQTT3AuthResult.newBuilder()
                                .setReject(Reject.newBuilder()
                                    .setCode(Reject.Code.Error)
                                    .setReason(e.getMessage())
                                    .build())
                                .build();
                        }
                    } else {
                        return MQTT3AuthResult.newBuilder()
                            .setReject(Reject.newBuilder()
                                .setCode(Reject.Code.Error)
                                .setReason("Authenticate failed")
                                .build())
                            .build();
                    }
                })
                .exceptionally(e -> {
                    System.out.println("Failed to call webhook: " + e.getMessage());
                    return null;
                });
        } catch (Throwable e) {
            return CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                    .setCode(Reject.Code.Error)
                    .setReason(e.getMessage())
                    .build())
                .build());
        }
    }

    @Override
    public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(webhookURI + "/check"))
                .header("Content-Type", "application/json")
                .header("tenant_id", client.getTenantId())
                .header("user_id", client.getMetadataMap().get(MQTT_USER_ID_KEY))
                .POST(HttpRequest.BodyPublishers.ofString(JsonFormat.printer().print(action)))
                .timeout(Duration.ofSeconds(5))
                .build();
            return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() == 200) {
                        try {
                            return Boolean.parseBoolean(response.body());
                        } catch (Throwable e) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                })
                .exceptionally(e -> {
                    System.out.println("Failed to call webhook: " + e.getMessage());
                    return null;
                });
        } catch (Throwable e) {
            return CompletableFuture.completedFuture(false);
        }
    }
}
