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

package org.apache.bifromq.plugin.authprovider;


import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Denied;
import org.apache.bifromq.plugin.authprovider.type.Error;
import org.apache.bifromq.plugin.authprovider.type.Failed;
import org.apache.bifromq.plugin.authprovider.type.Granted;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.Success;
import org.apache.bifromq.type.ClientInfo;
import java.util.concurrent.CompletableFuture;
import org.pf4j.ExtensionPoint;

public interface IAuthProvider extends ExtensionPoint {
    /**
     * Implement this method to hook authentication logic of mqtt3 client into BifroMQ.
     *
     * @param authData the authentication data
     */
    CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData);

    /**
     * Implement this method to hook authentication logic of mqtt5 client into BifroMQ. The default implementation will
     * delegate to the auth method of mqtt3.
     *
     * @param authData the authentication data
     * @return the authentication result
     */
    default CompletableFuture<MQTT5AuthResult> auth(MQTT5AuthData authData) {
        MQTT3AuthData.Builder mqtt3AuthDataBuilder = MQTT3AuthData.newBuilder();
        if (authData.hasCert()) {
            mqtt3AuthDataBuilder.setCert(authData.getCert());
        }
        if (authData.hasUsername()) {
            mqtt3AuthDataBuilder.setUsername(authData.getUsername());
        }
        if (authData.hasPassword()) {
            mqtt3AuthDataBuilder.setPassword(authData.getPassword());
        }
        if (authData.hasClientId()) {
            mqtt3AuthDataBuilder.setClientId(authData.getClientId());
        }
        mqtt3AuthDataBuilder.setRemoteAddr(authData.getRemoteAddr());
        mqtt3AuthDataBuilder.setRemotePort(authData.getRemotePort());
        mqtt3AuthDataBuilder.setClientId(authData.getClientId());
        return auth(mqtt3AuthDataBuilder.build()).thenApply(mqtt3AuthResult -> {
            MQTT5AuthResult.Builder mqtt5AuthResultBuilder = MQTT5AuthResult.newBuilder();
            switch (mqtt3AuthResult.getTypeCase()) {
                case OK -> {
                    mqtt5AuthResultBuilder.setSuccess(Success.newBuilder()
                        .setTenantId(mqtt3AuthResult.getOk().getTenantId())
                        .setUserId(mqtt3AuthResult.getOk().getUserId())
                        .putAllAttrs(mqtt3AuthResult.getOk().getAttrsMap())
                        .build());
                }
                case REJECT -> {
                    Failed.Builder failedBuilder = Failed.newBuilder();
                    switch (mqtt3AuthResult.getReject().getCode()) {
                        case BadPass -> failedBuilder.setCode(Failed.Code.BadPass);
                        case NotAuthorized -> failedBuilder.setCode(Failed.Code.NotAuthorized);
                        case Error -> failedBuilder.setCode(Failed.Code.Error);
                    }
                    if (mqtt3AuthResult.getReject().hasReason()) {
                        failedBuilder.setReason(mqtt3AuthResult.getReject().getReason());
                    }
                    mqtt5AuthResultBuilder.setFailed(failedBuilder.build());
                }
            }
            return mqtt5AuthResultBuilder.build();
        });
    }

    /**
     * Implement this method to hook extended authentication logic of mqtt5 client into BifroMQ.
     *
     * @param authData the extended authentication data
     * @return the authentication result
     */
    default CompletableFuture<MQTT5ExtendedAuthResult> extendedAuth(MQTT5ExtendedAuthData authData) {
        return CompletableFuture.completedFuture(MQTT5ExtendedAuthResult.newBuilder()
            .setFailed(Failed.newBuilder()
                .setCode(Failed.Code.NotAuthorized)
                .setReason("Not supported")
                .build())
            .build());
    }

    /**
     * Implement this method to hook action permission check logic.
     *
     * @param client the client to check permission
     * @param action the action
     * @return true if the client is allowed to perform the action, false otherwise
     */
    @Deprecated(since = "3.0")
    CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action);

    /**
     * Implement this method to hook action permission check logic.
     *
     * @param client the client to check permission
     * @param action the action
     * @return CheckResult
     */
    default CompletableFuture<CheckResult> checkPermission(ClientInfo client, MQTTAction action) {
        return check(client, action)
            .handle((granted, e) -> {
                if (e != null) {
                    return CheckResult.newBuilder()
                        .setError(Error.newBuilder()
                            .setReason(e.getMessage())
                            .build())
                        .build();
                } else if (granted) {
                    return CheckResult.newBuilder()
                        .setGranted(Granted.getDefaultInstance())
                        .build();
                } else {
                    return CheckResult.newBuilder()
                        .setDenied(Denied.getDefaultInstance())
                        .build();
                }
            });
    }

    /**
     * This method will be called during broker shutdown.
     */
    default void close() {
    }
}
