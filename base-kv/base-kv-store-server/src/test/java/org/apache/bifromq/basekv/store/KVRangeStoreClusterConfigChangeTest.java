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

package org.apache.bifromq.basekv.store;

import static java.util.Collections.emptySet;
import static org.awaitility.Awaitility.await;

import org.apache.bifromq.basekv.annotation.Cluster;
import org.apache.bifromq.basekv.proto.KVRangeId;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreClusterConfigChangeTest extends KVRangeStoreClusterTestTemplate {
    @Test(groups = "integration")
    public void removeNonLeaderReplicaFromNonLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig setting = await().until(() -> cluster.kvRangeSetting(rangeId), obj ->
            obj != null && obj.clusterConfig.getVotersCount() == 3);
        log.info("Start to change config");
        try {
            String remainStore = nonLeaderStore(setting);
            cluster.changeReplicaConfig(remainStore, setting.ver, rangeId, followStores(setting), emptySet())
                .toCompletableFuture().join();
        } catch (Throwable e) {
            log.info("Change config failed", e);
        }

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            return newSetting.clusterConfig.getVotersCount() == 2;
        });
    }

    @Test(groups = "integration")
    public void removeNonLeaderReplicaFromHostingStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig setting = cluster.awaitAllKVRangeReady(rangeId, 1, 40);
        String leaderStore = setting.leader;
        String remainStore = nonLeaderStore(setting);
        String removedStore = followStores(setting)
            .stream()
            .filter(storeId -> !storeId.equals(remainStore))
            .collect(Collectors.joining());

        log.info("Remove replica[{}]", removedStore);
        try {
            cluster.changeReplicaConfig(remainStore, setting.ver, rangeId, Sets.newHashSet(leaderStore, remainStore),
                emptySet()).toCompletableFuture().join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            return newSetting.clusterConfig.getVotersCount() == 2 &&
                !newSetting.clusterConfig.getVotersList().contains(removedStore);
        });
    }

    @Test(groups = "integration")
    public void removeLeaderReplicaFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = await().until(() -> cluster.kvRangeSetting(rangeId), Objects::nonNull);
        String leaderStore = rangeSettings.leader;
        Set<String> remainStores = followStores(rangeSettings);
        log.info("Remove: {}, remain: {}", leaderStore, remainStores);
        cluster.changeReplicaConfig(leaderStore, rangeSettings.ver, rangeId, Sets.newHashSet(remainStores), emptySet())
            .toCompletableFuture().join();

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            return remainStores.containsAll(setting.clusterConfig.getVotersList());
        });
    }

    @Test(groups = "integration")
    public void removeFailedReplica() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.kvRangeSetting(rangeId);
        String leaderStore = rangeSettings.leader;
        String failureStore = nonLeaderStore(rangeSettings);
        log.info("shutdown store {}", failureStore);
        cluster.shutdownStore(failureStore);

        List<String> remainStores = Lists.newArrayList(rangeSettings.clusterConfig.getVotersList());
        remainStores.remove(failureStore);
        log.info("Remain: {}", remainStores);
        cluster.changeReplicaConfig(leaderStore, rangeSettings.ver, rangeId, Sets.newHashSet(remainStores), emptySet())
            .toCompletableFuture().join();

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            return remainStores.containsAll(setting.clusterConfig.getVotersList());
        });
    }

    @Test(groups = "integration")
    public void removeNonLeaderReplicaFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 1, 40);
        String leaderStore = rangeSettings.leader;
        String remainStore = nonLeaderStore(rangeSettings);
        try {
            cluster.changeReplicaConfig(remainStore, rangeSettings.ver, rangeId,
                Sets.newHashSet(leaderStore, remainStore),
                emptySet()).toCompletableFuture().join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            return newSetting.clusterConfig.getVotersCount() == 2 &&
                newSetting.clusterConfig.getVotersList().contains(leaderStore) &&
                newSetting.clusterConfig.getVotersList().contains(remainStore);
        });
    }

    @Test(groups = "integration")
    public void removeLeaderReplicaFromNonLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.kvRangeSetting(rangeId);
        String leaderStore = rangeSettings.leader;
        List<String> remainStores = Lists.newArrayList(rangeSettings.clusterConfig.getVotersList());
        remainStores.remove(leaderStore);
        log.info("Try to remove leader: {}, remains: {}", leaderStore, remainStores);
        try {
            cluster.changeReplicaConfig(remainStores.get(0), rangeSettings.ver, rangeId, Sets.newHashSet(remainStores),
                emptySet()).toCompletableFuture().join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            return newSetting.clusterConfig.getVotersCount() == 2 &&
                !newSetting.clusterConfig.getVotersList().contains(leaderStore);
        });
    }

    @Cluster(initVoters = 1)
    @Test(groups = "integration")
    public void addReplicaFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        String newStore = cluster.addStore();
        log.info("add replica {}", newStore);
        KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
        cluster.changeReplicaConfig(setting.leader, setting.ver, rangeId, Sets.newHashSet(setting.leader, newStore),
            emptySet()).toCompletableFuture().join();

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            return newSetting.clusterConfig.getVotersCount() == 2;
        });
    }

    @Cluster(initVoters = 2)
    @Test(groups = "integration")
    public void addReplicaFromNonLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 1, 40);
        String newStore = cluster.addStore();
        Set<String> newReplicas = Sets.newHashSet(rangeSettings.clusterConfig.getVotersList());
        newReplicas.add(newStore);

        String nonLeaderStore = nonLeaderStore(rangeSettings);
        log.info("add replica {}, leader is {}, non-leader is {}", newStore, rangeSettings.leader, nonLeaderStore);
        try {
            cluster.changeReplicaConfig(nonLeaderStore, rangeSettings.ver, rangeId, newReplicas, emptySet())
                .toCompletableFuture().join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            return setting.clusterConfig.getVotersCount() == 3 &&
                setting.clusterConfig.getVotersList().contains(newStore);
        });
    }

    @Test(groups = "integration")
    public void jointChangeReplicasFromLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 1, 40);
        String newStore1 = cluster.addStore();
        String newStore2 = cluster.addStore();
        String newStore3 = cluster.addStore();
        Set<String> newReplicas = Sets.newHashSet(newStore1, newStore2, newStore3);
        log.info("Config change from {} to {}", rangeSettings.clusterConfig.getVotersList(), newReplicas);
        cluster.changeReplicaConfig(rangeSettings.leader, rangeSettings.ver, rangeId, newReplicas, emptySet())
            .toCompletableFuture().join();

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            return newReplicas.containsAll(setting.clusterConfig.getVotersList());
        });
        await().until(
            () -> rangeSettings.clusterConfig.getVotersList().stream()
                .noneMatch(storeId -> cluster.isHosting(storeId, rangeId)));
        log.info("Test done");
    }

    @Cluster(initVoters = 1)
    @Test(groups = "integration")
    public void moveHostingStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 0, 40);
        String newStore = cluster.addStore();
        Set<String> newReplicas = Sets.newHashSet(newStore);
        log.info("Config change from {} to {}", rangeSettings.clusterConfig.getVotersList(), newReplicas);
        await().ignoreExceptions().until(() -> {
            KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
            if (newReplicas.containsAll(setting.clusterConfig.getVotersList())) {
                return true;
            }
            cluster.changeReplicaConfig(setting.leader, setting.ver, rangeId, newReplicas, emptySet())
                .toCompletableFuture().join();
            setting = cluster.kvRangeSetting(rangeId);
            return newReplicas.containsAll(setting.clusterConfig.getVotersList());
        });
        await().until(
            () -> rangeSettings.clusterConfig.getVotersList().stream()
                .noneMatch(storeId -> cluster.isHosting(storeId, rangeId)));
        log.info("Test done");
    }

    @Test(groups = "integration")
    public void jointChangeReplicasFromNonLeaderStore() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 1, 40);
        String newStore1 = cluster.addStore();
        String newStore2 = cluster.addStore();
        String newStore3 = cluster.addStore();
        Set<String> newReplicas = Sets.newHashSet(newStore1, newStore2, newStore3);

        log.info("Joint-Config change to {}", newReplicas);
        try {
            cluster.changeReplicaConfig(nonLeaderStore(rangeSettings), rangeSettings.ver, rangeId, newReplicas,
                emptySet()).toCompletableFuture().join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig newSettings = cluster.kvRangeSetting(rangeId);
            return newReplicas.containsAll(newSettings.clusterConfig.getVotersList());
        });
        await().until(
            () -> rangeSettings.clusterConfig.getVotersList().stream()
                .noneMatch(storeId -> cluster.isHosting(storeId, rangeId)));
        log.info("Test done");

    }

    @Cluster(initVoters = 1)
    @Test(groups = "integration")
    public void gracefulQuit() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        KVRangeConfig rangeSettings = cluster.awaitAllKVRangeReady(rangeId, 0, 40);
        log.info("Graceful quit");
        cluster.changeReplicaConfig(rangeSettings.leader, rangeSettings.ver, rangeId, emptySet(), emptySet())
            .toCompletableFuture().join();
        await().until(() -> !cluster.isHosting(rangeSettings.leader, rangeId));
        log.info("Test done");
    }

    @Cluster(initVoters = 1)
    @Test(groups = "integration")
    public void configChangeDoNotAffectMutation() {
        KVRangeId rangeId = cluster.genesisKVRangeId();
        String newStore = cluster.addStore();
        KVRangeConfig setting = cluster.kvRangeSetting(rangeId);
        long ver = setting.ver;
        cluster.changeReplicaConfig(setting.leader, setting.ver, rangeId, Sets.newHashSet(setting.leader, newStore),
            emptySet()).toCompletableFuture().join();
        await().ignoreExceptions().atMost(40, TimeUnit.SECONDS).until(() -> {
            KVRangeConfig newSetting = cluster.kvRangeSetting(rangeId);
            return newSetting.clusterConfig.getVotersCount() == 2;
        });

        cluster.put(setting.leader, ver, rangeId, ByteString.copyFromUtf8("key"), ByteString.copyFromUtf8("value"))
            .toCompletableFuture().join();
    }
}
