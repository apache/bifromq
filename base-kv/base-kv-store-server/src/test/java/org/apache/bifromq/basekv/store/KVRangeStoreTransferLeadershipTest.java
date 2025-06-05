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

package org.apache.bifromq.basekv.store;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.exception.LeaderTransferException;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreTransferLeadershipTest extends KVRangeStoreClusterTestTemplate {

    @Test(groups = "integration")
    public void testRequestTransferLeadershipFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        await().until(() -> {
            KVRangeConfig settings = cluster.kvRangeSetting(rangeId);
            return settings != null && settings.ver >= 2;
        });
        KVRangeConfig rangeSettings = cluster.kvRangeSetting(rangeId);
        String oldLeader = rangeSettings.leader;
        String newLeader = nonLeaderStore(rangeSettings);
        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (setting.leader.equals(newLeader)) {
                return true;
            }
            cluster.transferLeader(oldLeader, setting.ver, rangeId, newLeader)
                .toCompletableFuture()
                .orTimeout(30, TimeUnit.SECONDS)
                .join();
            setting = cluster.kvRangeSetting(rangeId);
            return setting.leader.equals(newLeader);
        });
    }

    @Test(groups = "integration")
    public void testTransferLeadershipToFakeLeader() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSetting = cluster.kvRangeSetting(rangeId);
        String oldLeader = rangeSetting.leader;

        try {
            cluster.transferLeader(oldLeader, rangeSetting.ver, rangeId, "FakeLeader")
                .toCompletableFuture()
                .orTimeout(30, TimeUnit.SECONDS)
                .join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof KVRangeException.BadRequest);
        }
    }

    @Test(groups = "integration")
    public void testTransferLeadershipToSelf() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        await().until(() -> {
            KVRangeConfig settings = cluster.kvRangeSetting(rangeId);
            return settings != null && settings.ver >= 2;
        });
        await().ignoreExceptions().until(() -> {
            KVRangeConfig rangeSettings = cluster.kvRangeSetting(rangeId);
            try {
                cluster.transferLeader(rangeSettings.leader, rangeSettings.ver, rangeId, rangeSettings.leader)
                    .toCompletableFuture().join();
            } catch (Throwable e) {
                return e.getCause().getCause() instanceof LeaderTransferException.SelfTransferException;
            }
            KVRangeConfig newRangeSettings = cluster.kvRangeSetting(rangeId);
            return newRangeSettings.leader.equals(rangeSettings.leader);
        });
    }

    @Test(groups = "integration")
    public void testTransferLeadershipConcurrently() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        await().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            return setting != null && setting.ver >= 2;
        });
        KVRangeConfig rangeSettings = cluster.kvRangeSetting(rangeId);
        String oldLeader = rangeSettings.leader;
        String newLeader = nonLeaderStore(rangeSettings);

        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (setting.leader.equals(newLeader)) {
                return true;
            }
            cluster.transferLeader(oldLeader, setting.ver, rangeId, newLeader).toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return setting.leader.equals(newLeader);
        });
        // transfer back
        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (setting.leader.equals(oldLeader)) {
                return true;
            }
            cluster.transferLeader(newLeader, setting.ver, rangeId, oldLeader).toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return setting.leader.equals(oldLeader);
        });
    }
}
