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

package ${package};

import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.Error;
import org.apache.bifromq.plugin.authprovider.type.Failed;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.plugin.authprovider.type.Reject;
import org.apache.bifromq.type.ClientInfo;
import java.util.concurrent.CompletableFuture;
import org.pf4j.Extension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Extension
public class ${pluginName}AuthProvider implements IAuthProvider {
    private static final Logger log = LoggerFactory.getLogger(${pluginName}AuthProvider.class);

    public ${pluginName}AuthProvider(${pluginContextName} context) {
        log.info("TODO: Initialize your AuthProvider using context: {}", context);
    }

    @Override
    public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
        return CompletableFuture.completedFuture(MQTT3AuthResult.newBuilder()
                .setReject(Reject.newBuilder()
                        .setCode(Reject.Code.Error)
                        .setReason("Unimplemented")
                        .build())
                .build());
    }

    @Override
    public CompletableFuture<MQTT5AuthResult> auth(MQTT5AuthData authData) {
        return CompletableFuture.completedFuture(MQTT5AuthResult.newBuilder()
                .setFailed(Failed.newBuilder()
                        .setCode(Failed.Code.Banned)
                        .setReason("Unimplemented")
                        .build())
                .build());
    }

    @Override
    public CompletableFuture<MQTT5ExtendedAuthResult> extendedAuth(MQTT5ExtendedAuthData authData) {
        return CompletableFuture.completedFuture(MQTT5ExtendedAuthResult.newBuilder()
                .setFailed(Failed.newBuilder()
                        .setCode(Failed.Code.Banned)
                        .setReason("Unimplemented")
                        .build())
                .build());
    }

    @Override
    public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Unimplemented"));
    }

    @Override
    public CompletableFuture<CheckResult> checkPermission(ClientInfo client, MQTTAction action) {
        return CompletableFuture.completedFuture(CheckResult.newBuilder()
                .setError(Error.newBuilder()
                        .setReason("Unimplemented")
                        .build())
                .build());
    }
}