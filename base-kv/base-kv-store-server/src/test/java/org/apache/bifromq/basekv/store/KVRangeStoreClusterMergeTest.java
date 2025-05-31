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

import static org.apache.bifromq.basekv.proto.State.StateType.Merged;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compare;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.Collections.emptySet;
import static org.awaitility.Awaitility.await;

import org.apache.bifromq.basekv.annotation.Cluster;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreClusterMergeTest extends KVRangeStoreClusterTestTemplate {
    @Cluster(initVoters = 1)
    @Test(groups = "integration")
    public void mergeSingleNodeCluster() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 0, 40);
        log.info("Splitting range");
        cluster.split(genesisKVRangeSettings.leader,
                genesisKVRangeSettings.ver,
                genesisKVRangeId,
                copyFromUtf8("e"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(100)).until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig range0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig range1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (range0.boundary.hasEndKey() &&
            compare(range0.boundary.getEndKey(), range1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(range0);
            mergee = new AtomicReference<>(range1);
        } else {
            merger = new AtomicReference<>(range1);
            mergee = new AtomicReference<>(range0);
        }
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            log.info("Transfer mergee {} leader from {} to {}", KVRangeIdUtil.toString(mergee.get().id),
                mergee.get().leader,
                merger.get().leader);
            try {
                await().ignoreExceptions().atMost(Duration.ofSeconds(5)).until(() -> {
                    cluster.transferLeader(mergee.get().leader,
                            mergee.get().ver,
                            mergee.get().id,
                            merger.get().leader)
                        .toCompletableFuture().join();
                    return cluster.kvRangeSetting(mergee.get().id).leader.equals(merger.get().leader);
                });
                break;
            } catch (Throwable e) {
                log.info("Transfer failed, try again");
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }
        log.info("Merge KVRange[{}] to KVRange[{}] from leader store[{}]",
            KVRangeIdUtil.toString(mergee.get().id),
            KVRangeIdUtil.toString(merger.get().id),
            merger.get().leader);
        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
        cluster.merge(merger.get().leader,
                merger.get().ver,
                merger.get().id,
                mergee.get().id)
            .toCompletableFuture().join();

        KVRangeConfig mergerSetting = cluster.awaitAllKVRangeReady(merger.get().id, 3, 40);
        log.info("Merged settings {}", mergerSetting);
        await().atMost(Duration.ofSeconds(400)).until(() -> {
            for (String storeId : cluster.allStoreIds()) {
                KVRangeDescriptor mergeeDesc = cluster.getKVRange(storeId, mergee.get().id);
                if (mergeeDesc.getState() != Merged) {
                    return false;
                }
            }
            return true;
        });
        log.info("Merge done, and quit");
        KVRangeConfig mergeeSetting = cluster.awaitAllKVRangeReady(mergee.get().id, 3, 40);
        String lastStore = mergeeSetting.leader;
        cluster.changeReplicaConfig(lastStore, mergeeSetting.ver, mergee.get().id, emptySet(), emptySet())
            .toCompletableFuture().join();
        await().until(() -> !cluster.isHosting(lastStore, mergee.get().id));
        log.info("Test done");
    }

    @Test(groups = "integration")
    public void mergeFromLeaderStore() {
        merge();
    }

    @Cluster(initLearners = 1)
    @Test(groups = "integration")
    public void mergeWithLearner() {
        merge();
    }

    private void merge() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
        log.info("Splitting range");
        cluster.split(genesisKVRangeSettings.leader,
                genesisKVRangeSettings.ver,
                genesisKVRangeId,
                copyFromUtf8("e"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(100)).until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig range0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig range1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (range0.boundary.hasEndKey()
            && compare(range0.boundary.getEndKey(), range1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(range0);
            mergee = new AtomicReference<>(range1);
        } else {
            merger = new AtomicReference<>(range1);
            mergee = new AtomicReference<>(range0);
        }
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            log.info("Transfer mergee {} leader from {} to {}", KVRangeIdUtil.toString(mergee.get().id),
                mergee.get().leader,
                merger.get().leader);
            try {
                await().ignoreExceptions().atMost(Duration.ofSeconds(5)).until(() -> {
                    cluster.transferLeader(mergee.get().leader,
                            mergee.get().ver,
                            mergee.get().id,
                            merger.get().leader)
                        .toCompletableFuture().join();
                    return cluster.kvRangeSetting(mergee.get().id).leader.equals(merger.get().leader);
                });
                break;
            } catch (Throwable e) {
                log.info("Transfer failed, try again");
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }
        log.info("Merge KVRange[{}] to KVRange[{}] from leader store[{}]",
            KVRangeIdUtil.toString(mergee.get().id),
            KVRangeIdUtil.toString(merger.get().id),
            merger.get().leader);
        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
        cluster.merge(merger.get().leader,
                merger.get().ver,
                merger.get().id,
                mergee.get().id)
            .toCompletableFuture().join();

        KVRangeConfig mergerSetting = cluster.awaitAllKVRangeReady(merger.get().id, 6, 40);
        log.info("Merged settings {}", mergerSetting);
        await().atMost(Duration.ofSeconds(400)).until(() -> {
            for (String storeId : cluster.allStoreIds()) {
                KVRangeDescriptor mergeeDesc = cluster.getKVRange(storeId, mergee.get().id);
                if (mergeeDesc.getState() != Merged) {
                    return false;
                }
            }
            return true;
        });
        log.info("Merge done, and quit all mergee replicas");
        KVRangeConfig mergeeSetting = cluster.awaitAllKVRangeReady(mergee.get().id, 6, 40);
        cluster.changeReplicaConfig(mergeeSetting.leader,
                mergeeSetting.ver,
                mergeeSetting.id,
                Set.of(mergeeSetting.leader),
                emptySet())
            .toCompletableFuture().join();
        ClusterConfig clusterConfig = mergeeSetting.clusterConfig;
        Set<String> removedMergees = Sets.newHashSet(clusterConfig.getVotersList());
        removedMergees.remove(mergeeSetting.leader);
        await().until(
            () -> removedMergees.stream().noneMatch(storeId -> cluster.isHosting(storeId, mergee.get().id)));

        log.info("Quit last mergee replica");
        String lastStore = mergeeSetting.leader;
        mergeeSetting = cluster.awaitKVRangeReady(lastStore, mergee.get().id, 9);
        cluster.changeReplicaConfig(lastStore, mergeeSetting.ver, mergee.get().id, emptySet(), emptySet())
            .toCompletableFuture().join();
        await().until(() -> !cluster.isHosting(lastStore, mergee.get().id));
        log.info("Test done");
    }

    @Test(groups = "integration")
    public void mergeUnderOnlyQuorumAvailable() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
        log.info("Splitting range");
        cluster.split(genesisKVRangeSettings.leader,
                genesisKVRangeSettings.ver,
                genesisKVRangeId,
                copyFromUtf8("e"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(100)).until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig range0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig range1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (range0.boundary.hasEndKey()
            && compare(range0.boundary.getEndKey(), range1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(range0);
            mergee = new AtomicReference<>(range1);
        } else {
            merger = new AtomicReference<>(range1);
            mergee = new AtomicReference<>(range0);
        }
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            log.info("Transfer mergee {} leader from {} to {}", KVRangeIdUtil.toString(mergee.get().id),
                mergee.get().leader,
                merger.get().leader);
            try {
                await().ignoreExceptions().atMost(Duration.ofSeconds(5)).until(() -> {
                    cluster.transferLeader(mergee.get().leader,
                            mergee.get().ver,
                            mergee.get().id,
                            merger.get().leader)
                        .toCompletableFuture().join();
                    return cluster.kvRangeSetting(mergee.get().id).leader.equals(merger.get().leader);
                });
                break;
            } catch (Throwable e) {
                log.info("Transfer failed, try again");
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }
        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);

        String followerStoreId = cluster.allStoreIds().stream()
            .filter(s -> !s.equals(merger.get().leader)).collect(Collectors.toList()).get(0);
        log.info("Shutdown one store {}", followerStoreId);
        cluster.shutdownStore(followerStoreId);

        log.info("Merge KVRange {} to {} from leader store {}",
            KVRangeIdUtil.toString(mergee.get().id),
            KVRangeIdUtil.toString(merger.get().id),
            merger.get().leader);

        cluster.merge(merger.get().leader, merger.get().ver, merger.get().id, mergee.get().id)
            .toCompletableFuture().join();

        KVRangeConfig mergedSettings = cluster.awaitAllKVRangeReady(merger.get().id, 3, 40);
        log.info("Merged settings {}", mergedSettings);
        await().atMost(Duration.ofSeconds(40))
            .until(() -> cluster.kvRangeSetting(merger.get().id).boundary.equals(FULL_BOUNDARY));
        log.info("Merge done");
    }


    @Cluster(installSnapshotTimeoutTick = 10)
    @Test(groups = "integration")
    public void mergeWithOneMemberIsolated() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
        log.info("Splitting range");
        cluster.split(genesisKVRangeSettings.leader,
                genesisKVRangeSettings.ver,
                genesisKVRangeId,
                copyFromUtf8("e"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(100)).until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig range0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig range1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (range0.boundary.hasEndKey() &&
            compare(range0.boundary.getEndKey(), range1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(range0);
            mergee = new AtomicReference<>(range1);
        } else {
            merger = new AtomicReference<>(range1);
            mergee = new AtomicReference<>(range0);
        }
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            log.info("Transfer mergee {} leader from {} to {}",
                KVRangeIdUtil.toString(mergee.get().id),
                mergee.get().leader,
                merger.get().leader);
            try {
                await().ignoreExceptions().atMost(Duration.ofSeconds(5)).until(() -> {
                    cluster.transferLeader(mergee.get().leader,
                            mergee.get().ver,
                            mergee.get().id,
                            merger.get().leader)
                        .toCompletableFuture().join();
                    return Objects.equals(cluster.kvRangeSetting(mergee.get().id).leader, merger.get().leader);
                });
                break;
            } catch (Throwable e) {
                log.info("Transfer failed, try again");
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }
        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);

        String isolatedStoreId = cluster.allStoreIds().stream()
            .filter(s -> !s.equals(merger.get().leader)).collect(Collectors.toList()).get(0);
        log.info("Isolate one store {}", isolatedStoreId);
        cluster.isolate(isolatedStoreId);

        log.info("Merge KVRange {} to {} from leader store {}",
            KVRangeIdUtil.toString(mergee.get().id),
            KVRangeIdUtil.toString(merger.get().id),
            merger.get().leader);

        cluster.merge(merger.get().leader, merger.get().ver, merger.get().id, mergee.get().id)
            .toCompletableFuture().join();

        KVRangeConfig mergedSettings = cluster.awaitAllKVRangeReady(merger.get().id, 3, 40);
        await().atMost(Duration.ofSeconds(40))
            .until(() -> cluster.kvRangeSetting(merger.get().id).boundary.equals(FULL_BOUNDARY));
        log.info("Merge done {}", mergedSettings);
        log.info("Integrate {} into cluster, and wait for all mergees quited", isolatedStoreId);
        cluster.integrate(isolatedStoreId);
        await().atMost(Duration.ofSeconds(400)).until(() -> {
            for (String storeId : cluster.allStoreIds()) {
                KVRangeDescriptor mergeeDesc = cluster.getKVRange(storeId, mergee.get().id);
                if (mergeeDesc.getState() != Merged) {
                    return false;
                }
            }
            return true;
        });
    }
}
