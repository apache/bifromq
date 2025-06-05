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

package org.apache.bifromq.mqtt.service;

import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@Setter
public class LocalSessionServerBuilder implements ILocalSessionServerBuilder {
    RPCServerBuilder rpcServerBuilder;
    ILocalSessionRegistry sessionRegistry;
    ILocalDistService distService;
    Map<String, String> attributes = new HashMap<>();
    Set<String> defaultGroupTags = new HashSet<>();
    Executor rpcExecutor = MoreExecutors.directExecutor();

    public ILocalSessionServer build() {
        return new LocalSessionServer(this);
    }
}
