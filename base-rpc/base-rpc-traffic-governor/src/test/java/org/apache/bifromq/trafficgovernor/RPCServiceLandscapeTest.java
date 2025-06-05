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

package org.apache.bifromq.trafficgovernor;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;

import io.grpc.inprocess.InProcessSocketAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceLandscape;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceServerRegister;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.baserpc.trafficgovernor.ServerEndpoint;
import org.testng.annotations.Test;

@Slf4j
public class RPCServiceLandscapeTest extends RPCServiceAnnouncerTest {
    @Test(groups = "integration")
    public void startAndStop() {
        ICRDTService crdtService = newCRDTService(clientAgentHost);
        IRPCServiceServerRegister serverRegister = IRPCServiceTrafficService.newInstance(crdtService)
            .getServerRegister("serviceAAA");
        IRPCServiceServerRegister.IServerRegistration reg =
            serverRegister.reg("server1", new InetSocketAddress("127.0.0.1", 90));
        reg.stop();
    }

    @Test(groups = "integration")
    public void localServerDiscovery() {
        String service = "service";
        String server = "server";
        InetSocketAddress hostAddr = new InetSocketAddress("127.0.0.1", 90);
        ICRDTService crdtService = newCRDTService(clientAgentHost);
        IRPCServiceTrafficService trafficService = IRPCServiceTrafficService.newInstance(crdtService);
        IRPCServiceServerRegister serverRegister = trafficService.getServerRegister(service);
        IRPCServiceServerRegister.IServerRegistration serverReg = serverRegister.reg(server, hostAddr);
        IRPCServiceLandscape serviceLandscape = trafficService.getServiceLandscape(service);
        await().until(() -> {
            Set<ServerEndpoint> servers = serviceLandscape.serverEndpoints().blockingFirst();
            return servers.stream()
                .anyMatch(s -> s.id().equals(server) && s.hostAddr() instanceof InProcessSocketAddress);
        });

        // stop the server
        serverReg.stop();
        await().until(() -> {
            Set<ServerEndpoint> servers = serviceLandscape.serverEndpoints().blockingFirst();
            return servers.isEmpty();
        });

        trafficService.close();
        crdtService.close();
    }

    @Test(groups = "integration")
    public void remoteServerDiscovery() {
        String service = "service";
        String server = "server";
        InetSocketAddress hostAddr = new InetSocketAddress("127.0.0.1", 90);

        ICRDTService clientCrdtService = newCRDTService(clientAgentHost);
        IRPCServiceTrafficService clientTrafficService = IRPCServiceTrafficService.newInstance(clientCrdtService);
        IRPCServiceLandscape trafficDirector = clientTrafficService.getTrafficGovernor(service);
        assertTrue(trafficDirector.serverEndpoints().blockingFirst().isEmpty());

        // start a server
        ICRDTService serverCrdtService = newCRDTService(serverAgentHost);
        IRPCServiceTrafficService serverTrafficService = IRPCServiceTrafficService.newInstance(serverCrdtService);

        IRPCServiceServerRegister serverRegister = serverTrafficService.getServerRegister(service);
        IRPCServiceServerRegister.IServerRegistration serverReg = serverRegister.reg(server, hostAddr);

        // new server discovered
        await().until(() -> {
            Set<ServerEndpoint> servers = trafficDirector.serverEndpoints().blockingFirst();
            return servers.stream()
                .anyMatch(s -> s.id().equals(server) && s.hostAddr() instanceof InProcessSocketAddress);
        });
        // stop the server
        serverReg.stop();
        serverTrafficService.close();
        serverCrdtService.close();

        await().until(() -> {
            Set<ServerEndpoint> servers = trafficDirector.serverEndpoints().blockingFirst();
            return servers.isEmpty();
        });

        // start a server again
        serverCrdtService = newCRDTService(serverAgentHost);
        serverTrafficService = IRPCServiceTrafficService.newInstance(serverCrdtService);
        serverRegister = serverTrafficService.getServerRegister(service);
        serverRegister.reg(server, hostAddr);

        // server discovered again
        await().until(() -> {
            Set<ServerEndpoint> servers = trafficDirector.serverEndpoints().blockingFirst();
            return servers.stream()
                .anyMatch(s -> s.id().equals(server) && s.hostAddr() instanceof InProcessSocketAddress);
        });

        clientTrafficService.close();
        clientCrdtService.close();

        // stop the server
        serverTrafficService.close();
        serverCrdtService.close();
    }
}
