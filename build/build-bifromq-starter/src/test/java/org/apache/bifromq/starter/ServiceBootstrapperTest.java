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

package org.apache.bifromq.starter;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.apache.bifromq.apiserver.IAPIServer;
import org.apache.bifromq.baserpc.server.IRPCServer;
import org.apache.bifromq.baserpc.server.RPCServerBuilder;
import org.apache.bifromq.mqtt.IMQTTBroker;
import org.apache.bifromq.starter.module.ServiceInjector;
import org.mockito.InOrder;
import org.testng.annotations.Test;

public class ServiceBootstrapperTest {
    @Test
    public void mqttBrokerStartsRpcServer() {
        ServiceInjector serviceInjector = mock(ServiceInjector.class);
        RPCServerBuilder rpcServerBuilder = mock(RPCServerBuilder.class);
        IRPCServer rpcServer = mock(IRPCServer.class);
        IMQTTBroker mqttBroker = mock(IMQTTBroker.class);
        when(serviceInjector.getInstance(RPCServerBuilder.class)).thenReturn(rpcServerBuilder);
        when(rpcServerBuilder.build()).thenReturn(rpcServer);

        ServiceBootstrapper.BootstrappedServices services = new ServiceBootstrapper(
            Optional.empty(),
            Optional.of(mqttBroker),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            serviceInjector).bootstrap();

        services.start();

        InOrder startOrder = inOrder(rpcServer, mqttBroker);
        startOrder.verify(rpcServer).start();
        startOrder.verify(mqttBroker).start();

        services.stop();

        InOrder stopOrder = inOrder(mqttBroker, rpcServer);
        stopOrder.verify(mqttBroker).close();
        stopOrder.verify(rpcServer).shutdown();
    }

    @Test
    public void apiServerOnlyDoesNotStartRpcServer() {
        ServiceInjector serviceInjector = mock(ServiceInjector.class);
        IAPIServer apiServer = mock(IAPIServer.class);

        ServiceBootstrapper.BootstrappedServices services = new ServiceBootstrapper(
            Optional.of(apiServer),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            serviceInjector).bootstrap();

        services.start();

        verify(apiServer).start();
        verify(serviceInjector, never()).getInstance(RPCServerBuilder.class);
    }
}
