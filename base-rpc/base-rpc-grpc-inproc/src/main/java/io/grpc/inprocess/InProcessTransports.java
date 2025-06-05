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

package io.grpc.inprocess;

import io.grpc.internal.ClientTransportFactory;
import io.grpc.internal.ConnectionClientTransport;

/**
 * Factory for creating in-process transports.
 */
public class InProcessTransports {

    /**
     * Creates a new in-process transport.
     * @param serverAddress the address of the server to connect to
     * @param options the transport options
     * @return a new in-process transport
     */
    public static ConnectionClientTransport create(InProcessSocketAddress serverAddress,
                                                   ClientTransportFactory.ClientTransportOptions options) {
        return new InProcessTransport(
            serverAddress, Integer.MAX_VALUE, options.getAuthority(), options.getUserAgent(),
            options.getEagAttributes(), false, -1);
    }
}
