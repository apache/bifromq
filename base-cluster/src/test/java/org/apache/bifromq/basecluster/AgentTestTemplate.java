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

package org.apache.bifromq.basecluster;

import java.lang.reflect.Method;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecluster.annotation.StoreCfg;
import org.apache.bifromq.basecluster.annotation.StoreCfgs;
import org.apache.bifromq.basecrdt.store.CRDTStoreOptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

@Slf4j
public abstract class AgentTestTemplate {
    protected AgentTestCluster storeMgr;

    AgentTestTemplate() {
    }

    public void createClusterByAnnotation(Method testMethod) {
        StoreCfgs storeCfgs = testMethod.getAnnotation(StoreCfgs.class);
        StoreCfg storeCfg = testMethod.getAnnotation(StoreCfg.class);
        String seedStoreId = null;
        if (storeMgr != null) {
            if (storeCfgs != null) {
                for (StoreCfg cfg : storeCfgs.stores()) {
                    storeMgr.registerHost(cfg.id(), build(cfg));
                    storeMgr.startHost(cfg.id());
                    if (cfg.isSeed()) {
                        seedStoreId = cfg.id();
                    }
                }
            }
            if (storeCfg != null) {
                storeMgr.registerHost(storeCfg.id(), build(storeCfg));
                storeMgr.startHost(storeCfg.id());
            }
            if (seedStoreId != null && storeCfgs != null) {
                for (StoreCfg cfg : storeCfgs.stores()) {
                    if (!cfg.id().equals(seedStoreId)) {
                        storeMgr.join(cfg.id(), seedStoreId);
                    }
                }
            }
        }
    }

    @BeforeMethod()
    public void setup(Method method) {
        log.info("Test case[{}.{}] start", method.getDeclaringClass().getName(), method.getName());

        storeMgr = new AgentTestCluster();
        createClusterByAnnotation(method);
    }

    @AfterMethod()
    public void teardown(Method method) {
        log.info("Test case[{}.{}] finished, doing teardown",
            method.getDeclaringClass().getName(), method.getName());

        if (storeMgr != null) {
            log.info("Shutting down test cluster");
            AgentTestCluster lastStoreMgr = this.storeMgr;
            lastStoreMgr.shutdown();
        }
    }

    private AgentHostOptions build(StoreCfg cfg) {
        return AgentHostOptions.builder()
            .udpPacketLimit(1400)
            .maxChannelsPerHost(1)
            .joinRetryInSec(cfg.joinRetryInSec())
            .joinTimeout(Duration.ofSeconds(cfg.joinTimeout()))
            .autoHealingTimeout(Duration.ofSeconds(60))
            .baseProbeTimeout(Duration.ofMillis(cfg.baseProbeTimeoutMillis()))
            .baseProbeInterval(Duration.ofMillis(cfg.baseProbeIntervalMillis()))
            .gossipPeriod(Duration.ofMillis(cfg.baseGossipIntervalMillis()))
            .crdtStoreOptions(CRDTStoreOptions.builder()
                .orHistoryExpireTime(Duration.ofSeconds(cfg.compactDelayInSec()))
                .maxEventsInDelta(100)
                .build()
            ).build();
    }
}
