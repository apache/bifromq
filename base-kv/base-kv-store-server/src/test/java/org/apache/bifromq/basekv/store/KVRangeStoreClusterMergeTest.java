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
import static org.apache.bifromq.basekv.proto.State.StateType.Merged;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.combine;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compare;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.annotation.Cluster;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeStoreClusterMergeTest extends KVRangeStoreClusterTestTemplate {
    @Cluster(initVoters = 1)
    @Test(groups = "integration")
    public void mergeSingleNodeCluster() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 0, 40);
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
        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
        cluster.merge(merger.get().leader,
                merger.get().ver,
                merger.get().id,
                mergee.get().id)
            .toCompletableFuture().join();
        KVRangeConfig mergeeSetting = cluster.awaitAllKVRangeReady(mergee.get().id, 3, 40);
        String lastStore = mergeeSetting.leader;
        cluster.changeReplicaConfig(lastStore, mergeeSetting.ver, mergee.get().id, emptySet(), emptySet())
            .toCompletableFuture().join();
        await().until(() -> !cluster.isHosting(lastStore, mergee.get().id));
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
        cluster.merge(merger.get().leader,
                merger.get().ver,
                merger.get().id,
                mergee.get().id)
            .toCompletableFuture().join();

        KVRangeConfig mergerSetting = cluster.awaitAllKVRangeReady(merger.get().id, 6, 40);
        await().atMost(Duration.ofSeconds(400)).until(() -> {
            for (String storeId : cluster.allStoreIds()) {
                KVRangeDescriptor mergeeDesc = cluster.getKVRange(storeId, mergee.get().id);
                // tolerate null as already quit; non-null must be Merged
                if (mergeeDesc != null && mergeeDesc.getState() != Merged) {
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
    }

    @Test(groups = "integration")
    public void mergeUnderOnlyQuorumAvailable() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
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

        cluster.merge(merger.get().leader, merger.get().ver, merger.get().id, mergee.get().id)
            .toCompletableFuture().join();
        KVRangeConfig mergedSettings = cluster.awaitAllKVRangeReady(merger.get().id, 3, 40);
        log.info("Merged settings {}", mergedSettings);
        await().atMost(Duration.ofSeconds(40))
            .until(() -> cluster.kvRangeSetting(merger.get().id).boundary.equals(FULL_BOUNDARY));
        log.info("Merge done");
    }

    @Test(groups = "integration")
    public void crossNetworkDataMigration() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
        // Split into two adjacent ranges
        cluster.split(genesisKVRangeSettings.leader,
                genesisKVRangeSettings.ver,
                genesisKVRangeId,
                copyFromUtf8("m"))
            .toCompletableFuture().join();
        await().atMost(Duration.ofSeconds(100)).until(() -> cluster.allKVRangeIds().size() == 2);

        KVRangeConfig range0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig range1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (compare(range0.boundary, range1.boundary) <= 0) {
            merger = new AtomicReference<>(range0); // left
            mergee = new AtomicReference<>(range1); // right
        } else {
            merger = new AtomicReference<>(range1);
            mergee = new AtomicReference<>(range0);
        }

        // Ensure cross-network migration by moving mergee's voters to a disjoint set of stores
        Set<String> mergerVoters = Set.copyOf(merger.get().clusterConfig.getVotersList());
        Set<String> oldMergeeVoters = Set.copyOf(mergee.get().clusterConfig.getVotersList());
        int voterCount = mergerVoters.size();
        Set<String> newMergeeVoters = Sets.newHashSet();
        while (newMergeeVoters.size() < voterCount) {
            String s = cluster.addStore();
            if (!mergerVoters.contains(s)) {
                newMergeeVoters.add(s);
            }
        }
        KVRangeConfig meCfg = mergee.get();
        cluster.changeReplicaConfig(meCfg.leader, meCfg.ver, meCfg.id, newMergeeVoters, emptySet())
            .toCompletableFuture().join();
        // wait until all replicas in old stores are removed
        await().ignoreExceptions().forever().until(
            () -> oldMergeeVoters.stream().map(store -> cluster.getKVRange(store, meCfg.id)).allMatch(Objects::isNull));

        // wait until all stores reflect the new voters config for mergee
        await().ignoreExceptions().atMost(Duration.ofSeconds(60)).until(() -> {
            for (String store : cluster.allStoreIds()) {
                KVRangeDescriptor rd = cluster.getKVRange(store, meCfg.id);
                if (rd == null) {
                    continue;
                }
                Set<String> votersOnStore = new HashSet<>(rd.getConfig().getVotersList());
                if (!votersOnStore.containsAll(newMergeeVoters)) {
                    return false;
                }
            }
            return true;
        });
        mergee.set(cluster.kvRangeSetting(meCfg.id));

        // Ensure mergee elects a leader among new voters before writing
        await().ignoreExceptions().atMost(Duration.ofSeconds(30))
            .until(() -> newMergeeVoters.contains(cluster.kvRangeSetting(mergee.get().id).leader));
        mergee.set(cluster.kvRangeSetting(mergee.get().id));
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);

        // Put some keys in mergee range (>= "m")
        String[] keys = {"n1", "n2", "z"};
        for (String k : keys) {
            cluster.put(mergee.get().leader, mergee.get().id, copyFromUtf8(k), copyFromUtf8("v" + k));
        }

        // Deterministically ensure MergeDone reaches the migrated mergee leader:
        // Hold MergeDoneRequest to mergee until merger finishes combining boundaries, then release.
        try (KVRangeStoreTestCluster.HoldHandle holdMergeDone =
                 cluster.holdIf(m -> m.getPayload().hasMergeDoneRequest()
                     && m.getPayload().getRangeId().equals(mergee.get().id))) {
            // Trigger merge
            KVRangeConfig mBefore = cluster.kvRangeSetting(merger.get().id);
            KVRangeConfig meBefore = cluster.kvRangeSetting(mergee.get().id);
            // Use explicit mergee voters to avoid stale config being captured in MergeRequest
            cluster.mergeWithMergeeVoters(merger.get().leader, mBefore.ver, mBefore.id, meBefore.id, newMergeeVoters)
                .toCompletableFuture().join();

            // Wait for merger to return to Normal with combined boundary
            await().atMost(Duration.ofSeconds(120)).until(() -> {
                KVRangeConfig mNow = cluster.kvRangeSetting(merger.get().id);
                return Objects.equals(mNow.boundary, combine(mBefore.boundary, meBefore.boundary));
            });

            // Ensure mergee has a leader among new voters, then release MergeDone
            await().ignoreExceptions().atMost(Duration.ofSeconds(30))
                .until(() -> newMergeeVoters.contains(cluster.kvRangeSetting(mergee.get().id).leader));
            holdMergeDone.releaseAll();
        }

        // Mergee becomes Merged across all stores
        await().atMost(Duration.ofSeconds(120)).until(() ->
            cluster.allStoreIds().stream().map(s -> cluster.getKVRange(s, mergee.get().id))
                .filter(Objects::nonNull)
                .allMatch(desc -> desc.getState() == Merged));

        // Keys from mergee should be readable via merger
        for (String k : keys) {
            var val = cluster.get(merger.get().leader, merger.get().id, copyFromUtf8(k));
            assert val.isPresent();
        }
    }

    @Cluster(installSnapshotTimeoutTick = 10)
    @Test(groups = "integration")
    public void mergeWithOneMemberIsolated() {
        KVRangeId genesisKVRangeId = cluster.genesisKVRangeId();
        KVRangeConfig genesisKVRangeSettings = cluster.awaitAllKVRangeReady(genesisKVRangeId, 1, 40);
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
                // tolerate null as already quit; non-null must be Merged
                if (mergeeDesc != null && mergeeDesc.getState() != Merged) {
                    return false;
                }
            }
            return true;
        });
    }
}
