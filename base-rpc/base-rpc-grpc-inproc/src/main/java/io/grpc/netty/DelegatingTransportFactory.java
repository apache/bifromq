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

import io.grpc.ChannelCredentials;
import io.grpc.ChannelLogger;
import io.grpc.inprocess.InProcessSocketAddress;
import io.grpc.inprocess.InProcessTransports;
import io.grpc.internal.ClientTransportFactory;
import io.grpc.internal.ConnectionClientTransport;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

public class DelegatingTransportFactory implements ClientTransportFactory {
    private final ClientTransportFactory delegate;

    public DelegatingTransportFactory(NettyChannelBuilder builder) {
        this.delegate = builder.buildTransportFactory();
    }

    @Override
    public ConnectionClientTransport newClientTransport(
        SocketAddress address, ClientTransportOptions options, ChannelLogger logger) {
        if (address instanceof InProcessSocketAddress inProcSocketAddress) {
            return InProcessTransports.create(inProcSocketAddress, options);
        }
        return delegate.newClientTransport(address, options, logger);
    }

    @Override
    public ScheduledExecutorService getScheduledExecutorService() {
        return delegate.getScheduledExecutorService();
    }

    @Override
    public SwapChannelCredentialsResult swapChannelCredentials(ChannelCredentials channelCreds) {
        return delegate.swapChannelCredentials(channelCreds);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Collection<Class<? extends SocketAddress>> getSupportedSocketAddressTypes() {
        return Collections.singleton(SocketAddress.class);
    }
}
