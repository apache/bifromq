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

import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.Collections.emptySet;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basekv.proto.KVRangeId;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreClusterRWTest extends KVRangeStoreClusterTestTemplate {
    @Test(groups = "integration")
    public void readFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSetting = cluster.awaitAllKVRangeReady(rangeId, 1, 40);
        for (int i = 0; i < 10; i++) {
            cluster.put(rangeSetting.leader, rangeId, copyFromUtf8("key" + i), copyFromUtf8("value" + i));
            Optional<ByteString> getValue = cluster.get(rangeSetting.leader, rangeId, copyFromUtf8("key" + i));
            assertEquals(getValue.get(), copyFromUtf8("value" + i));
        }
    }

    @Test(groups = "integration")
    public void readFromNonLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSetting = cluster.awaitAllKVRangeReady(rangeId, 1, 40);
        for (int i = 0; i < 10; i++) {
            cluster.put(rangeSetting.leader, rangeId, copyFromUtf8("key" + i), copyFromUtf8("value" + i));
            Optional<ByteString> getValue = cluster.get(nonLeaderStore(rangeSetting), rangeId, copyFromUtf8("key" + i));
            assertEquals(getValue.get(), copyFromUtf8("value" + i));
        }
    }

    @Test(groups = "integration")
    public void readWhenReplicaRestart() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSetting = cluster.awaitAllKVRangeReady(rangeId, 1, 40);
        String restartStoreId = nonLeaderStore(rangeSetting);

        log.info("Shutdown store {}", restartStoreId);
        cluster.shutdownStore(restartStoreId);
        for (int i = 0; i < 10; i++) {
            cluster.put(rangeSetting.leader, rangeId, copyFromUtf8("key" + i), copyFromUtf8("value" + i));
            Optional<ByteString> resp = cluster.get(rangeSetting.leader, rangeId, copyFromUtf8("key" + i));
            assertEquals(resp.get(), copyFromUtf8("value" + i));
        }
        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            cluster.changeReplicaConfig(rangeSetting.leader,
                setting.ver,
                rangeId,
                Sets.newHashSet(cluster.allStoreIds()),
                emptySet()
            ).toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return setting.clusterConfig.getVotersCount() == 2 &&
                !setting.clusterConfig.getVotersList().contains(restartStoreId);
        });
        log.info("Restart store {}", restartStoreId);
        cluster.startStore(restartStoreId);

        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            cluster.changeReplicaConfig(setting.leader,
                setting.ver,
                rangeId,
                Sets.newHashSet(cluster.allStoreIds()),
                emptySet()
            ).toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return setting.clusterConfig.getVotersCount() == 3 &&
                setting.clusterConfig.getVotersList().contains(restartStoreId);
        });

        cluster.awaitKVRangeReady(restartStoreId, rangeId);
        for (int i = 0; i < 10; i++) {
            Optional<ByteString> resp = cluster.get(restartStoreId, rangeId, copyFromUtf8("key" + i));
            assertEquals(resp.get(), copyFromUtf8("value" + i));
        }
    }

    @Test(groups = "integration")
    public void readWhileAddNewReplica() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 1, 40);

        for (int i = 0; i < 10; i++) {
            cluster.put(rangeSettings.leader, rangeId, copyFromUtf8("key" + i), copyFromUtf8("value" + i));
            Optional<ByteString> resp = cluster.get(rangeSettings.leader, rangeId, copyFromUtf8("key" + i));
            assertEquals(resp.get(), copyFromUtf8("value" + i));
        }
        String storeId = cluster.addStore();
        log.info("Add new store: {}", storeId);
        log.info("Change replica set to: {}", cluster.allStoreIds());
        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            cluster.changeReplicaConfig(rangeSettings.leader,
                setting.ver,
                rangeId,
                Sets.newHashSet(cluster.allStoreIds()),
                Sets.newHashSet()
            ).toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return setting.clusterConfig.getVotersList().contains(storeId);
        });
        cluster.awaitKVRangeReady(storeId, rangeId);
        log.info("New store ready");
        for (int i = 0; i < 10; i++) {
            Optional<ByteString> resp = cluster.get(storeId, rangeId, copyFromUtf8("key" + i));
            assertEquals(resp.get(), copyFromUtf8("value" + i));
        }
    }
}
