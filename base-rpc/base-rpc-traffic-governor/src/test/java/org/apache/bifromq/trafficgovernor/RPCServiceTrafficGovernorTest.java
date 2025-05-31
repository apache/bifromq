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

package org.apache.bifromq.trafficgovernor;

import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.bifromq.basecrdt.service.ICRDTService;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceLandscape;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceServerRegister;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficGovernor;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.testng.annotations.Test;

public class RPCServiceTrafficGovernorTest extends RPCServiceAnnouncerTest {

    @Test(groups = "integration")
    public void defaultTrafficDirective() {
        String service = "service";
        ICRDTService crdtService = newCRDTService(clientAgentHost);
        IRPCServiceTrafficService trafficService = IRPCServiceTrafficService.newInstance(crdtService);
        IRPCServiceTrafficGovernor trafficGovernor = trafficService.getTrafficGovernor(service);
        assertTrue(trafficGovernor.trafficRules().blockingFirst().isEmpty());

        trafficService.close();
        crdtService.close();
    }

    @Test(groups = "integration")
    public void updateTrafficDirective() {
        String service = "service";
        Map<String, Map<String, Integer>> td = singletonMap("tenantA", singletonMap("group1", 1));
        ICRDTService tgCrdtService = newCRDTService(clientAgentHost);
        IRPCServiceTrafficService tgTrafficService = IRPCServiceTrafficService.newInstance(tgCrdtService);
        IRPCServiceTrafficGovernor trafficGovernor = tgTrafficService.getTrafficGovernor(service);

        ICRDTService slCrdtService = newCRDTService(serverAgentHost);
        IRPCServiceTrafficService slTrafficService = IRPCServiceTrafficService.newInstance(slCrdtService);
        IRPCServiceLandscape serviceLandscape = slTrafficService.getServiceLandscape(service);

        trafficGovernor.setTrafficRules("tenantA", singletonMap("group1", 1));

        await().until(() -> trafficGovernor.trafficRules().blockingFirst().equals(td)
            && serviceLandscape.trafficRules().blockingFirst().equals(td));

        Map<String, Map<String, Integer>> td1 =
            Map.of("tenantA", singletonMap("group1", 1), "tenantB", singletonMap("group2", 1));
        trafficGovernor.setTrafficRules("tenantB", singletonMap("group2", 1));

        await().until(() -> trafficGovernor.trafficRules().blockingFirst().equals(td1)
            && serviceLandscape.trafficRules().blockingFirst().equals(td1));

        trafficGovernor.unsetTrafficRules("tenantB");

        await().until(() -> trafficGovernor.trafficRules().blockingFirst().equals(td)
            && serviceLandscape.trafficRules().blockingFirst().equals(td));

        tgTrafficService.close();
        tgCrdtService.close();

        slTrafficService.close();
        slCrdtService.close();
    }

    @Test(groups = "integration")
    public void updateGroupAssignments() {
        String service = "service";
        String server = "server";
        String lbGroup = "group";
        InetSocketAddress hostAddr = new InetSocketAddress("127.0.0.1", 90);
        ICRDTService srCrdtService = newCRDTService(serverAgentHost);
        IRPCServiceTrafficService srTrafficService = IRPCServiceTrafficService.newInstance(srCrdtService);
        IRPCServiceServerRegister serverRegister = srTrafficService.getServerRegister(service);
        IRPCServiceServerRegister.IServerRegistration serverReg = serverRegister.reg(server, hostAddr);

        ICRDTService tgCrdtService = newCRDTService(governorAgentHost);
        IRPCServiceTrafficService tgTrafficService = IRPCServiceTrafficService.newInstance(tgCrdtService);
        IRPCServiceTrafficGovernor trafficGovernor = tgTrafficService.getTrafficGovernor(service);

        ICRDTService slCrdtService = newCRDTService(clientAgentHost);
        IRPCServiceTrafficService slTrafficService = IRPCServiceTrafficService.newInstance(slCrdtService);
        IRPCServiceLandscape serviceLandscape = slTrafficService.getServiceLandscape(service);
        await().until(() -> !trafficGovernor.serverEndpoints().blockingFirst().isEmpty());

        trafficGovernor.setServerGroups(server, Sets.newHashSet(lbGroup)).join();

        await().until(() -> trafficGovernor.serverEndpoints().blockingFirst().stream()
            .anyMatch(s -> s.groupTags().contains(lbGroup)) &&
            serviceLandscape.serverEndpoints().blockingFirst().stream().anyMatch(s -> s.groupTags().contains(lbGroup)));


        serverReg.stop();
        srTrafficService.close();
        tgTrafficService.close();
        slTrafficService.close();

        srCrdtService.close();
        tgCrdtService.close();
        slCrdtService.close();
    }
}
