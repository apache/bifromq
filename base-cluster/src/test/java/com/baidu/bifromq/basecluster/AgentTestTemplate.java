/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basecluster;

import com.baidu.bifromq.basecluster.annotation.StoreCfg;
import com.baidu.bifromq.basecluster.annotation.StoreCfgs;
import com.baidu.bifromq.basecrdt.core.api.CRDTEngineOptions;
import com.baidu.bifromq.basecrdt.store.CRDTStoreOptions;
import java.lang.reflect.Method;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
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
                    storeMgr.newHost(cfg.id(), build(cfg));
                    if (cfg.isSeed()) {
                        seedStoreId = cfg.id();
                    }
                }
            }
            if (storeCfg != null) {
                storeMgr.newHost(storeCfg.id(), build(storeCfg));
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
    public void setup() {
        storeMgr = new AgentTestCluster();
    }

    @AfterMethod()
    public void teardown() {
        if (storeMgr != null) {
            log.info("Shutting down test cluster");
            // run in a separate thread to avoid blocking the test thread
            new Thread(() -> storeMgr.shutdown()).start();
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
                .engineOptions(CRDTEngineOptions.builder()
                    .orHistoryExpireTime(Duration.ofSeconds(cfg.compactDelayInSec()))
                    .build())
                .maxEventsInDelta(100)
                .build()
            ).build();
    }
}
