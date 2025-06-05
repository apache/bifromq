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

package org.apache.bifromq.retain.client;

import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.retain.RPCBluePrint;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * The builder for building Retain Client.
 */
@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Setter
public final class RetainClientBuilder {
    IRPCServiceTrafficService trafficService;
    EventLoopGroup eventLoopGroup;
    SslContext sslContext;
    int workerThreads;

    public IRetainClient build() {
        return new RetainClient(IRPCClient.newBuilder()
            .bluePrint(RPCBluePrint.INSTANCE)
            .trafficService(trafficService)
            .workerThreads(workerThreads)
            .eventLoopGroup(eventLoopGroup)
            .sslContext(sslContext)
            .build());
    }
}
