/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

import com.baidu.bifromq.basecrdt.service.ICRDTService;
import io.netty.channel.EventLoopGroup;
import java.util.UUID;
import java.util.concurrent.Executor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
@Setter
public final class StandaloneMQTTBrokerBuilder extends AbstractMQTTBrokerBuilder<StandaloneMQTTBrokerBuilder> {
    String id = UUID.randomUUID().toString();
    String rpcHost;
    int rpcPort;
    ICRDTService crdtService;
    EventLoopGroup rpcBossGroup;
    EventLoopGroup rpcWorkerGroup;
    Executor ioExecutor;

    StandaloneMQTTBrokerBuilder() {
    }

    @Override
    public String brokerId() {
        return id;
    }

    public IMQTTBroker build() {
        return new StandaloneMQTTBroker(this);
    }
}
