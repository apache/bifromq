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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.apache.bifromq.basecrdt.core.api.CCounterOperation;
import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.ICCounter;
import org.apache.bifromq.basecrdt.service.annotation.ServiceCfg;
import org.apache.bifromq.basecrdt.service.annotation.ServiceCfgs;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CCounterTest extends CRDTServiceTestTemplate {
    @ServiceCfgs(services =
        {
            @ServiceCfg(id = "s1", isSeed = true),
            @ServiceCfg(id = "s2")
        })
    @Test(groups = "integration")
    public void testZeroOut() {
        ICRDTService service1 = testCluster.getService("s1");
        ICRDTService service2 = testCluster.getService("s2");
        String uri = CRDTURI.toURI(CausalCRDTType.cctr, "test");
        ICCounter counter1 = service1.host(uri);
        ICCounter counter2 = service2.host(uri);
        assertNotEquals(counter1.id(), counter2.id());
        awaitUntilTrue(() -> service1.aliveReplicas(uri).blockingFirst().size() == 2);
        awaitUntilTrue(() -> service2.aliveReplicas(uri).blockingFirst().size() == 2);

        counter1.execute(CCounterOperation.add(1)).join();
        counter2.execute(CCounterOperation.add(2)).join();
        awaitUntilTrue(() -> counter1.read() == counter2.read());
        Assert.assertEquals(counter1.read(), 3);

        counter2.execute(CCounterOperation.zeroOut()).join();
        awaitUntilTrue(() -> counter1.read() == counter2.read());
        awaitUntilTrue(() -> counter1.read() == 1);

        assertEquals(service1.aliveCRDTs().blockingFirst(), Set.of(uri));
        assertEquals(service1.aliveCRDTs().blockingFirst(), service2.aliveCRDTs().blockingFirst());

        service1.stopHosting(uri).join();
        service2.stopHosting(uri).join();
    }
}
