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

package org.apache.bifromq.basecrdt.service;

import org.apache.bifromq.basecrdt.core.api.CCounterOperation;
import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.ICCounter;
import org.apache.bifromq.basecrdt.service.annotation.ServiceCfg;
import org.apache.bifromq.basecrdt.service.annotation.ServiceCfgs;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class BasicCRDTObjectTest extends CRDTServiceTestTemplate {

    @ServiceCfgs(services =
        {
            @ServiceCfg(id = "s1", isSeed = true),
            @ServiceCfg(id = "s2")
        })
    @Test(groups = "integration")
    public void testConvergence() {
        ICRDTService service1 = testCluster.getService("s1");
        ICRDTService service2 = testCluster.getService("s2");
        String uri = CRDTURI.toURI(CausalCRDTType.cctr, "test");
        ICCounter counter1 = service1.host(uri);
        ICCounter counter2 = service2.host(uri);
        awaitUntilTrue(() -> service1.aliveReplicas(uri).blockingFirst().size() == 2);
        awaitUntilTrue(() -> service2.aliveReplicas(uri).blockingFirst().size() == 2);

        counter1.execute(CCounterOperation.add(1)).join();
        counter2.execute(CCounterOperation.add(2)).join();
        awaitUntilTrue(() -> counter1.read() == counter2.read());
        Assert.assertEquals(3, counter1.read());

        service1.stopHosting(uri).join();
        service2.stopHosting(uri).join();
    }

    @ServiceCfgs(services =
        {
            @ServiceCfg(id = "s1", isSeed = true),
            @ServiceCfg(id = "s2")
        })
    @Test(groups = "integration")
    public void testPartition() {
        ICRDTService service1 = testCluster.getService("s1");
        ICRDTService service2 = testCluster.getService("s2");
        String uri = CRDTURI.toURI(CausalCRDTType.cctr, "test");
        ICCounter counter1 = service1.host(uri);
        ICCounter counter2 = service2.host(uri);
        awaitUntilTrue(() -> service1.aliveReplicas(uri).blockingFirst().size() == 2);
        awaitUntilTrue(() -> service2.aliveReplicas(uri).blockingFirst().size() == 2);

        counter1.execute(CCounterOperation.add(1)).join();
        counter2.execute(CCounterOperation.add(2)).join();
        awaitUntilTrue(() -> counter1.read() == counter2.read());
        Assert.assertEquals(3, counter1.read());

        service2.stopHosting(uri).join();
        awaitUntilTrue(() -> service1.aliveReplicas(uri).blockingFirst().size() == 1);
        Assert.assertEquals(3, counter1.read());

        counter1.execute(CCounterOperation.zeroOut(counter2.id().getId())).join();
        Assert.assertEquals(1, counter1.read());

        service1.stopHosting(uri).join();
    }
}
