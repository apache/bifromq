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

package com.baidu.bifromq.dist.worker;

import com.baidu.bifromq.baserpc.RPCServerBuilder;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
@Accessors(fluent = true)
@Setter
public class NonStandaloneDistWorkerBuilder extends AbstractDistWorkerBuilder<NonStandaloneDistWorkerBuilder> {
    RPCServerBuilder rpcServerBuilder;

    @Override
    public IDistWorker build() {
        return new NonStandaloneDistWorker(this);
    }
}
