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

package io.grpc.netty;

import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.ManagedChannelImplBuilder;

/**
 * Wraps a {@link NettyChannelBuilder} to create a channel that can connect to an in-process server.
 */
public class InProcAware {
    /**
     * Wraps a {@link NettyChannelBuilder} to create a channel that can connect to an in-process server.
     *
     * @param target the target address of the server
     * @param builder the NettyChannelBuilder to use
     * @return a ManagedChannel that can be used to connect to the server
     */
    public static ManagedChannelBuilder<ManagedChannelImplBuilder> wrap(String target, NettyChannelBuilder builder) {
        return new ManagedChannelImplBuilder(
            target,
            () -> new DelegatingTransportFactory(builder),
            builder::getDefaultPort
        );
    }
}
