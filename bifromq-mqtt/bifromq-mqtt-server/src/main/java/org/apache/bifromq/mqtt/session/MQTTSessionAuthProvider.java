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

package org.apache.bifromq.mqtt.session;

import io.netty.channel.ChannelHandlerContext;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.plugin.authprovider.IAuthProvider;
import org.apache.bifromq.plugin.authprovider.type.CheckResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT3AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5AuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthData;
import org.apache.bifromq.plugin.authprovider.type.MQTT5ExtendedAuthResult;
import org.apache.bifromq.plugin.authprovider.type.MQTTAction;
import org.apache.bifromq.type.ClientInfo;

@Slf4j
public class MQTTSessionAuthProvider implements IAuthProvider {
    private final IAuthProvider delegate;
    private final ChannelHandlerContext ctx;
    private final LinkedHashMap<CompletableFuture<CheckResult>, CompletableFuture<CheckResult>>
        checkPermissionTaskQueue = new LinkedHashMap<>();

    public MQTTSessionAuthProvider(IAuthProvider delegate, ChannelHandlerContext ctx) {
        this.delegate = delegate;
        this.ctx = ctx;
    }

    @Override
    public CompletableFuture<MQTT3AuthResult> auth(MQTT3AuthData authData) {
        return delegate.auth(authData);
    }

    @Override
    public CompletableFuture<MQTT5AuthResult> auth(MQTT5AuthData authData) {
        return delegate.auth(authData);
    }

    @Override
    public CompletableFuture<MQTT5ExtendedAuthResult> extendedAuth(MQTT5ExtendedAuthData authData) {
        return delegate.extendedAuth(authData);
    }

    @Override
    public CompletableFuture<Boolean> check(ClientInfo client, MQTTAction action) {
        return delegate.check(client, action);
    }

    @Override
    public CompletableFuture<CheckResult> checkPermission(ClientInfo client, MQTTAction action) {
        assert ctx.executor().inEventLoop();
        // proactively drain head to minimize latency while keeping order
        drainInOrder();

        CompletableFuture<CheckResult> task = delegate.checkPermission(client, action);
        // fast path
        if (task.isDone() && checkPermissionTaskQueue.isEmpty()) {
            return task;
        }

        // queue it for strict FIFO semantics
        CompletableFuture<CheckResult> onDone = new CompletableFuture<>();
        // in case authProvider returns same future object
        task = task.thenApply(v -> v);
        checkPermissionTaskQueue.put(task, onDone);
        task.whenComplete((_v, _e) -> {
            if (ctx.executor().inEventLoop()) {
                // drain upon completion on event loop to release head in order
                drainInOrder();
            } else {
                // schedule a drain on event loop to release head in order
                ctx.executor().execute(this::drainInOrder);
            }
        });
        return onDone;
    }

    // drain head completed tasks in FIFO order
    private void drainInOrder() {
        Iterator<CompletableFuture<CheckResult>> itr = checkPermissionTaskQueue.keySet().iterator();
        while (itr.hasNext()) {
            CompletableFuture<CheckResult> k = itr.next();
            if (k.isDone()) {
                CompletableFuture<CheckResult> r = checkPermissionTaskQueue.get(k);
                try {
                    // complete in caller's event loop
                    r.complete(k.join());
                } catch (Throwable e) {
                    r.completeExceptionally(e);
                }
                itr.remove();
            } else {
                break;
            }
        }
    }
}
