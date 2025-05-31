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

package org.apache.bifromq.basekv.server;

import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import java.util.HashMap;
import java.util.Map;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@Setter
public class BaseKVStoreServerBuilder {
    final Map<String, BaseKVStoreServiceBuilder> serviceBuilders = new HashMap<>();
    RPCServerBuilder rpcServerBuilder;
    IBaseKVMetaService metaService;

    public BaseKVStoreServiceBuilder addService(String clusterId) {
        assert !serviceBuilders.containsKey(clusterId);
        return new BaseKVStoreServiceBuilder(clusterId, this);
    }

    public IBaseKVStoreServer build() {
        return new BaseKVStoreServer(this);
    }
}
