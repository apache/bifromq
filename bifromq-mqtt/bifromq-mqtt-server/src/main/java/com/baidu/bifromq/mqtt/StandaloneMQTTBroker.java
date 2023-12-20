/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package com.baidu.bifromq.mqtt;

import com.baidu.bifromq.mqtt.service.ILocalSessionRegistry;
import com.baidu.bifromq.mqtt.service.ILocalSessionServer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class StandaloneMQTTBroker extends AbstractMQTTBroker<StandaloneMQTTBrokerBuilder> {
    private final ILocalSessionServer sessionServer;

    public StandaloneMQTTBroker(StandaloneMQTTBrokerBuilder builder) {
        super(builder);
        sessionServer = ILocalSessionServer.standaloneBuilder()
                .id(builder.id)
                .host(builder.rpcHost)
                .port(builder.rpcPort)
                .executor(builder.ioExecutor)
                .bossEventLoopGroup(builder.rpcBossGroup)
                .workerEventLoopGroup(builder.rpcWorkerGroup)
                .crdtService(builder.crdtService)
                .build();
    }

    @Override
    protected ILocalSessionRegistry sessionRegistry() {
        return sessionServer;
    }

    @Override
    protected void beforeBrokerStart() {
        sessionServer.start();
    }

    @Override
    protected void afterBrokerStop() {
        sessionServer.shutdown();
    }
}
