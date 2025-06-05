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

package org.apache.bifromq.baserpc.client;

import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * The builder for creating RPC client.
 */
@Accessors(fluent = true)
@Setter
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public final class RPCClientBuilder {
    private IRPCServiceTrafficService trafficService;
    private BluePrint bluePrint;
    private int workerThreads;
    private EventLoopGroup eventLoopGroup;
    private long keepAliveInSec;
    private long idleTimeoutInSec;
    private SslContext sslContext;

    public RPCClientBuilder sslContext(SslContext sslContext) {
        if (sslContext != null) {
            Preconditions.checkArgument(sslContext.isClient(), "Client auth must be enabled");
        }
        this.sslContext = sslContext;
        return this;
    }

    public IRPCClient build() {
        return new RPCClient(bluePrint, ClientChannel.builder()
            .bluePrint(bluePrint)
            .trafficService(trafficService)
            .eventLoopGroup(eventLoopGroup)
            .sslContext(sslContext)
            .workerThreads(workerThreads)
            .keepAliveInSec(keepAliveInSec)
            .idleTimeoutInSec(idleTimeoutInSec)
            .build());
    }
}

