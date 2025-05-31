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

import static org.testng.Assert.fail;

import org.apache.bifromq.basecluster.AgentHostOptions;
import org.apache.bifromq.basecrdt.service.annotation.ServiceCfg;
import org.apache.bifromq.basecrdt.service.annotation.ServiceCfgs;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

@Slf4j
public class CRDTServiceTestTemplate {
    protected CRDTServiceTestCluster testCluster;

    public void createClusterByAnnotation(Method testMethod) {
        ServiceCfgs serviceCfgs = testMethod.getAnnotation(ServiceCfgs.class);
        ServiceCfg serviceCfg = testMethod.getAnnotation(ServiceCfg.class);
        String seedStoreId = null;
        if (testCluster != null) {
            if (serviceCfgs != null) {
                for (ServiceCfg cfg : serviceCfgs.services()) {
                    testCluster.newService(cfg.id(), buildHostOptions(cfg), buildCrdtServiceOptions(cfg));
                    if (cfg.isSeed()) {
                        seedStoreId = cfg.id();
                    }
                }
            }
            if (serviceCfg != null) {
                testCluster.newService(serviceCfg.id(),
                    buildHostOptions(serviceCfg),
                    buildCrdtServiceOptions(serviceCfg));
            }
            if (seedStoreId != null && serviceCfgs != null) {
                for (ServiceCfg cfg : serviceCfgs.services()) {
                    if (!cfg.id().equals(seedStoreId)) {
                        try {
                            testCluster.join(cfg.id(), seedStoreId);
                        } catch (Exception e) {
                            log.error("Join failed", e);
                        }
                    }
                }
            }
        }
    }

    @BeforeMethod(groups = "integration")
    public void setup(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());
        testCluster = new CRDTServiceTestCluster();
        createClusterByAnnotation(method);
    }

    @AfterMethod(groups = "integration")
    public void teardown(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
                method.getDeclaringClass().getName(), method.getName());
        if (testCluster != null) {
            log.info("Shutting down test cluster");
            CRDTServiceTestCluster lastStoreMgr = this.testCluster;
            new Thread(lastStoreMgr::shutdown).start();
        }
    }

    public void awaitUntilTrue(Callable<Boolean> condition) {
        awaitUntilTrue(condition, 5000);
    }

    public void awaitUntilTrue(Callable<Boolean> condition, long timeoutInMS) {
        try {
            long waitingTime = 0;
            while (!condition.call()) {
                Thread.sleep(100);
                waitingTime += 100;
                if (waitingTime > timeoutInMS) {
                    fail();
                }
            }
        } catch (Exception e) {
            fail();
        }
    }

    private CRDTServiceOptions buildCrdtServiceOptions(ServiceCfg cfg) {
        return new CRDTServiceOptions();
    }

    private AgentHostOptions buildHostOptions(ServiceCfg cfg) {
        // expose more options
        return new AgentHostOptions()
            .addr(cfg.bindAddr())
            .port(cfg.bindPort());
    }
}
