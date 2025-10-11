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
import static org.apache.bifromq.basekv.utils.BoundaryUtil.NULL_BOUNDARY;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.apache.bifromq.basekv.annotation.Cluster;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.store.exception.KVRangeException.BadRequest;
import org.apache.bifromq.basekv.store.exception.KVRangeException.TryLater;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.testng.annotations.Test;

public class KVRangeStoreClusterMergeEdgeCasesTest extends KVRangeStoreClusterTestTemplate {

    @Cluster(initVoters = 1)
    @Test(groups = "integration")
    public void mergeMissingRollback() {
        KVRangeId merger = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, merger);
        KVRangeConfig before = cluster.kvRangeSetting(merger);

        KVRangeId missing = KVRangeIdUtil.generate();

        try {
            cluster.merge(leader, before.ver, merger, missing).toCompletableFuture().join();
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause();
            assertTrue(cause instanceof BadRequest);
        }

        cluster.awaitKVRangeStateOnAllStores(merger, State.StateType.Normal, 60);
        KVRangeConfig after = cluster.kvRangeSetting(merger);
        assertEquals(before.ver, after.ver);
    }

    @SneakyThrows
    @Test(groups = "integration")
    public void mispreparedTimeoutConsistentState() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey() && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }

        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);

        KVRangeConfig before = cluster.kvRangeSetting(merger.get().id);

        // drop PreparedMergeToReply/MergeRequest/MergeHelpRequest so it must self-timeout
        try (AutoCloseable dropPMTReply = cluster.dropIf(m -> m.getPayload().hasPrepareMergeToReply()
            && m.getPayload().getRangeId().equals(merger.get().id));
             AutoCloseable dropMHRequest = cluster.dropIf(m -> m.getPayload().hasMergeHelpRequest());
             KVRangeStoreTestCluster.HoldHandle blockMergeReq = cluster.holdIf(m -> m.getPayload().hasMergeRequest()
                 && m.getPayload().getRangeId().equals(merger.get().id))) {
            try {
                cluster.merge(merger.get().leader, before.ver, merger.get().id, mergee.get().id)
                    .toCompletableFuture().join();
                fail();
            } catch (CompletionException ex) {
                assertTrue(ex.getCause() instanceof TryLater);
            }

            // Merger self-timeouts and cancels, stays with original boundary
            cluster.awaitKVRangeStateOnAllStores(merger.get().id, State.StateType.Normal, 60);
            KVRangeConfig after = cluster.kvRangeSetting(merger.get().id);
            assertTrue(after.ver >= before.ver + 1);
            assertEquals(after.boundary, before.boundary);

            // Release MergeRequest to arrive late;
            // merger(Normal) should send CancelMergingRequest, mergee back to Normal
            blockMergeReq.releaseAll();
            cluster.awaitKVRangeStateOnAllStores(mergee.get().id, State.StateType.Normal, 60);
        }
    }

    @Test(groups = "integration")
    public void prepareCondNotMetThenCancel() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey()
            && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }

        // Ensure merger and mergee are hosted on the same leader store,
        // so PrepareMergeTo is handled by the real mergee instead of the "absent mergee" fast path.
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            try {
                await().ignoreExceptions().atMost(java.time.Duration.ofSeconds(5))
                    .until(() -> {
                        cluster.transferLeader(mergee.get().leader, mergee.get().ver, mergee.get().id,
                                merger.get().leader)
                            .toCompletableFuture().join();
                        return cluster.kvRangeSetting(mergee.get().id).leader.equals(merger.get().leader);
                    });
                break;
            } catch (Throwable t) {
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }
        String s2 = cluster.addStore();
        KVRangeConfig mCfg = mergee.get();
        Set<String> voters = new HashSet<>(mCfg.clusterConfig.getVotersList());
        Set<String> learners = new HashSet<>(mCfg.clusterConfig.getLearnersList());
        learners.add(s2);
        cluster.changeReplicaConfig(mCfg.leader, mCfg.ver, mCfg.id, voters, learners).toCompletableFuture().join();

        try {
            cluster.merge(merger.get().leader, merger.get().ver, merger.get().id, mergee.get().id)
                .toCompletableFuture().join();
        } catch (CompletionException ex) {
            assertTrue(ex.getCause() instanceof TryLater);
        }

        cluster.awaitKVRangeStateOnAllStores(merger.get().id, State.StateType.Normal, 60);
        KVRangeConfig after = cluster.kvRangeSetting(merger.get().id);
        assertTrue(after.ver >= merger.get().ver + 1);
    }

    @Test(groups = "integration")
    public void mergeDoneRetryToMerged() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey()
            && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            try {
                await().ignoreExceptions().atMost(Duration.ofSeconds(5))
                    .until(() -> {
                        cluster.transferLeader(mergee.get().leader, mergee.get().ver, mergee.get().id,
                                merger.get().leader)
                            .toCompletableFuture().join();
                        return cluster.kvRangeSetting(mergee.get().id).leader.equals(merger.get().leader);
                    });
                break;
            } catch (Throwable t) {
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }

        try (AutoCloseable g = cluster.delayOnceIf(m ->
            m.getPayload().hasMergeDoneReply() && m.getPayload().getRangeId().equals(merger.get().id), 200)) {
            cluster.merge(merger.get().leader, merger.get().ver, merger.get().id, mergee.get().id)
                .toCompletableFuture().join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        cluster.awaitKVRangeStateOnAllStores(mergee.get().id, State.StateType.Merged, 60);
    }

    @Test(groups = "integration")
    public void nonAdjacentMergeeCanceledMerger() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        // first split at "m"
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig rA = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig rB = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        KVRangeConfig rightHalf = rA.boundary.hasStartKey() ? rA : rB;
        cluster.split(rightHalf.leader, rightHalf.ver, rightHalf.id,
            copyFromUtf8("t")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 3);

        KVRangeConfig c0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig c1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        KVRangeConfig c2 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(2));
        KVRangeConfig left = c0.boundary.hasStartKey() ? (c1.boundary.hasStartKey() ? c2 : c1) : c0;
        KVRangeConfig right;
        if (!c0.boundary.hasEndKey()) {
            right = c0;
        } else if (!c1.boundary.hasEndKey()) {
            right = c1;
        } else {
            right = c2;
        }

        AtomicReference<KVRangeConfig> merger = new AtomicReference<>(left);
        AtomicReference<KVRangeConfig> mergee = new AtomicReference<>(right);
        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);

        KVRangeConfig before = cluster.kvRangeSetting(merger.get().id);
        try {
            cluster.merge(merger.get().leader, before.ver, merger.get().id, mergee.get().id)
                .toCompletableFuture().join();
        } catch (CompletionException ex) {
            assertTrue(ex.getCause() instanceof TryLater);
        }

        cluster.awaitKVRangeStateOnAllStores(merger.get().id, State.StateType.Normal, 60);
        KVRangeConfig after = cluster.kvRangeSetting(merger.get().id);
        assertTrue(after.ver >= before.ver + 1);
        cluster.awaitKVRangeStateOnAllStores(mergee.get().id, State.StateType.Normal, 60);
    }

    @Test(groups = "integration")
    public void nonAdjacentMergerSelfCancel() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig rA = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig rB = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        KVRangeConfig rightHalf = rA.boundary.hasStartKey() ? rA : rB;
        cluster.split(rightHalf.leader, rightHalf.ver, rightHalf.id,
            copyFromUtf8("t")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 3);

        KVRangeConfig c0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig c1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        KVRangeConfig c2 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(2));
        KVRangeConfig left = c0.boundary.hasStartKey() ? (c1.boundary.hasStartKey() ? c2 : c1) : c0;
        KVRangeConfig right;
        if (!c0.boundary.hasEndKey()) {
            right = c0;
        } else if (!c1.boundary.hasEndKey()) {
            right = c1;
        } else {
            right = c2;
        }

        cluster.awaitAllKVRangeReady(left.id, left.ver, 40);
        cluster.awaitAllKVRangeReady(right.id, right.ver, 40);

        AtomicReference<KVRangeConfig> merger = new AtomicReference<>(left);
        AtomicReference<KVRangeConfig> mergee = new AtomicReference<>(right);
        KVRangeConfig before = cluster.kvRangeSetting(merger.get().id);

        // Drop all CancelMergingRequest to merger so it must self-timeout
        try (AutoCloseable dr = cluster.dropIf(m -> m.getPayload().hasCancelMergingRequest()
            && m.getPayload().getRangeId().equals(merger.get().id))) {
            try {
                cluster.merge(merger.get().leader, before.ver, merger.get().id, mergee.get().id)
                    .toCompletableFuture().join();
            } catch (CompletionException ex) {
                assertTrue(ex.getCause() instanceof TryLater);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        cluster.awaitKVRangeStateOnAllStores(merger.get().id, State.StateType.Normal, 60);
        KVRangeConfig after = cluster.kvRangeSetting(merger.get().id);
        assertTrue(after.ver >= before.ver + 1);
        cluster.awaitKVRangeStateOnAllStores(mergee.get().id, State.StateType.Normal, 60);
    }

    @Test(groups = "integration")
    @SneakyThrows
    public void mispreparedBothCanceled() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey() && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }

        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);

        KVRangeConfig before = cluster.kvRangeSetting(merger.get().id);

        AutoCloseable dropMHRequest = cluster.dropIf(m -> m.getPayload().hasMergeHelpRequest());
        // One-way isolation: merger cannot receive PrepareMergeToReply and MergeRequest
        try (AutoCloseable dropPMTReply = cluster.dropIf(m -> m.getPayload().hasPrepareMergeToReply()
            && m.getPayload().getRangeId().equals(merger.get().id))) {
            KVRangeStoreTestCluster.HoldHandle holdHandle = cluster.holdIf(m -> m.getPayload().hasMergeRequest()
                && m.getPayload().getRangeId().equals(merger.get().id));
            try {
                cluster.merge(merger.get().leader, before.ver, merger.get().id, mergee.get().id)
                    .toCompletableFuture().join();
            } catch (CompletionException ex) {
                assertTrue(ex.getCause() instanceof TryLater);
            }
            // Merger self-timeouts and cancels
            cluster.awaitKVRangeStateOnAllStores(merger.get().id, State.StateType.Normal, 60);
            KVRangeConfig after = cluster.kvRangeSetting(merger.get().id);
            assertTrue(after.ver >= before.ver + 1);
            assertEquals(after.boundary, before.boundary);
        }

        // Mergee should have at least one replica in WaitingForMerge (state split)
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            for (String sid : cluster.allStoreIds()) {
                KVRangeDescriptor rd = cluster.getKVRange(sid, mergee.get().id);
                if (rd != null && rd.getState() == State.StateType.WaitingForMerge) {
                    return true;
                }
            }
            return false;
        });
        dropMHRequest.close();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            for (String sid : cluster.allStoreIds()) {
                KVRangeDescriptor rd = cluster.getKVRange(sid, mergee.get().id);
                if (rd != null && rd.getState() == State.StateType.Normal
                    && rd.getBoundary().equals(mergee.get().boundary)) {
                    return true;
                }
            }
            return false;
        });
    }

    @Test(groups = "integration")
    public void mispreparedButMerged() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey() && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }

        cluster.awaitKVRangeReady(merger.get().leader, merger.get().id);
        cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);

        KVRangeConfig mBefore = cluster.kvRangeSetting(merger.get().id);
        KVRangeConfig meBefore = cluster.kvRangeSetting(mergee.get().id);

        // drop PrepareMergeToReply and MergeHelpRequest to try best to let merger receive MergeRequest first
        AutoCloseable dropMHRequest = cluster.dropIf(m -> m.getPayload().hasMergeHelpRequest());
        try (AutoCloseable dropPMTReply = cluster.dropIf(m -> m.getPayload().hasPrepareMergeToReply()
            && m.getPayload().getRangeId().equals(merger.get().id))) {
            cluster.merge(merger.get().leader, mBefore.ver, merger.get().id, mergee.get().id)
                .toCompletableFuture().join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        cluster.awaitKVRangeStateOnAllStores(merger.get().id, State.StateType.Normal, 120);
        KVRangeConfig mAfter = cluster.kvRangeSetting(merger.get().id);
        assertTrue(mAfter.ver >= mBefore.ver + 1);
        assertEquals(mAfter.boundary, BoundaryUtil.combine(mBefore.boundary, meBefore.boundary));

        cluster.awaitKVRangeStateOnAllStores(mergee.get().id, State.StateType.Merged, 120);
        KVRangeConfig meAfter = cluster.kvRangeSetting(mergee.get().id);
        assertTrue(meAfter.ver >= meBefore.ver + 1);
    }

    @Test(groups = "integration")
    @SneakyThrows
    public void mergeDoneLostAndMergeeTimeout() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey()
            && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }

        // Drop all MergeDoneRequest to mergee so it stays in WaitingForMerge
        try (AutoCloseable dropMergeDone = cluster.dropIf(m -> m.getPayload().hasMergeDoneRequest()
            && m.getPayload().getRangeId().equals(mergee.get().id))) {

            // Trigger merge
            KVRangeConfig mBefore = cluster.kvRangeSetting(merger.get().id);
            KVRangeConfig meBefore = cluster.kvRangeSetting(mergee.get().id);
            cluster.merge(merger.get().leader, mBefore.ver, mBefore.id, meBefore.id)
                .toCompletableFuture().join();

            // Wait for merger to return Normal with combined boundary
            await().atMost(Duration.ofSeconds(120)).until(() -> {
                KVRangeConfig mNow = cluster.kvRangeSetting(merger.get().id);
                return mNow.boundary.equals(BoundaryUtil.combine(mBefore.boundary, meBefore.boundary))
                    && mNow.ver >= mBefore.ver + 1;
            });

            await().atMost(Duration.ofSeconds(30)).until(() -> {
                for (String sid : cluster.allStoreIds()) {
                    KVRangeDescriptor rd = cluster.getKVRange(sid, mergee.get().id);
                    if (rd != null && rd.getState() == State.StateType.WaitingForMerge) {
                        return true;
                    }
                }
                return false;
            });
        }

        // Wait for mergee to return Merged state
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            for (String sid : cluster.allStoreIds()) {
                KVRangeDescriptor rd = cluster.getKVRange(sid, mergee.get().id);
                if (rd != null && rd.getState() == State.StateType.Merged && rd.getBoundary().equals(NULL_BOUNDARY)) {
                    return true;
                }
            }
            return false;
        });
    }

    @Cluster(initLearners = 1)
    @Test(groups = "integration")
    public void votersOnly() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey()
            && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }
        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            try {
                await().ignoreExceptions().atMost(Duration.ofSeconds(5))
                    .until(() -> {
                        cluster.transferLeader(mergee.get().leader, mergee.get().ver, mergee.get().id,
                                merger.get().leader)
                            .toCompletableFuture().join();
                        return cluster.kvRangeSetting(mergee.get().id).leader.equals(merger.get().leader);
                    });
                break;
            } catch (Throwable t) {
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }

        cluster.merge(merger.get().leader, merger.get().ver, merger.get().id, mergee.get().id)
            .toCompletableFuture().join();

        cluster.awaitKVRangeStateOnAllStores(mergee.get().id, State.StateType.Merged, 60);
    }

    @Test(groups = "integration")
    public void followersTimeoutThenResetBySnapshot() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid,
            copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey()
            && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }

        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            try {
                await().ignoreExceptions().atMost(Duration.ofSeconds(5))
                    .until(() -> {
                        cluster.transferLeader(mergee.get().leader, mergee.get().ver, mergee.get().id,
                                merger.get().leader)
                            .toCompletableFuture().join();
                        return cluster.kvRangeSetting(mergee.get().id).leader.equals(merger.get().leader);
                    });
                break;
            } catch (Throwable t) {
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }

        KVRangeConfig mBefore = cluster.kvRangeSetting(merger.get().id);
        KVRangeConfig meBefore = cluster.kvRangeSetting(mergee.get().id);

        try (AutoCloseable dropDMRFromFollowers = cluster.dropIf(m ->
            m.getPayload().hasDataMergeRequest()
                && m.getPayload().getRangeId().equals(mergee.get().id)
                && !m.getFrom().equals(merger.get().leader))) {
            cluster.merge(merger.get().leader, mBefore.ver, mBefore.id, meBefore.id)
                .toCompletableFuture().join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        cluster.awaitKVRangeStateOnAllStores(merger.get().id, State.StateType.Normal, 120);

        assertEquals(BoundaryUtil.combine(mBefore.boundary, meBefore.boundary),
            cluster.kvRangeSetting(merger.get().id).boundary);
        Boundary expected = BoundaryUtil.combine(mBefore.boundary, meBefore.boundary);
        await().untilAsserted(() -> {
            for (String sid : cluster.allStoreIds()) {
                KVRangeDescriptor rd = cluster.getKVRange(sid, merger.get().id);
                if (rd != null) {
                    assertEquals(rd.getState(), State.StateType.Normal);
                    assertEquals(rd.getBoundary(), expected);
                }
            }
        });

        cluster.awaitKVRangeStateOnAllStores(mergee.get().id, State.StateType.Merged, 60);
        await().untilAsserted(() -> {
            for (String sid : cluster.allStoreIds()) {
                KVRangeDescriptor rd = cluster.getKVRange(sid, mergee.get().id);
                if (rd != null && rd.getState() == State.StateType.Merged) {
                    assertEquals(rd.getBoundary(), NULL_BOUNDARY);
                }
            }
        });
    }

    @Test(groups = "integration")
    public void oneFollowerCompletesLeaderAndOtherReset() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid, copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);
        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey()
            && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }

        while (!merger.get().leader.equals(mergee.get().leader)) {
            cluster.awaitKVRangeReady(mergee.get().leader, mergee.get().id);
            try {
                await().ignoreExceptions().atMost(Duration.ofSeconds(5))
                    .until(() -> {
                        cluster.transferLeader(mergee.get().leader, mergee.get().ver, mergee.get().id,
                                merger.get().leader)
                            .toCompletableFuture().join();
                        return cluster.kvRangeSetting(mergee.get().id).leader.equals(merger.get().leader);
                    });
                break;
            } catch (Throwable t) {
                merger.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(0)));
                mergee.set(cluster.kvRangeSetting(cluster.allKVRangeIds().get(1)));
            }
        }

        KVRangeConfig mBefore = cluster.kvRangeSetting(merger.get().id);
        KVRangeConfig meBefore = cluster.kvRangeSetting(mergee.get().id);

        String mergerLeader = mBefore.leader;
        String allowedFollower = followStores(mBefore).iterator().next();

        try (AutoCloseable dropAllButOne = cluster.dropIf(m ->
            m.getPayload().hasDataMergeRequest()
                && m.getPayload().getRangeId().equals(mergee.get().id)
                && !m.getFrom().equals(allowedFollower))) {
            try {
                cluster.merge(mergerLeader, mBefore.ver, mBefore.id, meBefore.id)
                    .toCompletableFuture().join();
            } catch (CompletionException ex) {
                assertTrue(ex.getCause() instanceof TryLater);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        cluster.awaitKVRangeStateOnAllStores(merger.get().id, State.StateType.Normal, 120);

        Boundary expected = BoundaryUtil.combine(mBefore.boundary, meBefore.boundary);
        assertEquals(expected, cluster.kvRangeSetting(merger.get().id).boundary);
        for (String sid : cluster.allStoreIds()) {
            KVRangeDescriptor rd = cluster.getKVRange(sid, merger.get().id);
            if (rd != null) {
                assertEquals(rd.getState(), State.StateType.Normal);
                assertEquals(rd.getBoundary(), expected);
            }
        }

        cluster.awaitKVRangeStateOnAllStores(mergee.get().id, State.StateType.Merged, 60);
        for (String sid : cluster.allStoreIds()) {
            KVRangeDescriptor rd = cluster.getKVRange(sid, mergee.get().id);
            if (rd != null && rd.getState() == State.StateType.Merged) {
                assertEquals(rd.getBoundary(), NULL_BOUNDARY);
            }
        }
    }

    @SneakyThrows
    @Cluster(mergeTimeoutSec = 1)
    @Test(groups = "integration")
    public void tryMigrateFailedThenCancelSeesMerged() {
        KVRangeId gid = cluster.genesisKVRangeId();
        String leader = cluster.bootstrapStore();
        cluster.awaitKVRangeReady(leader, gid);
        cluster.split(leader, cluster.kvRangeSetting(gid).ver, gid, copyFromUtf8("m")).toCompletableFuture().join();
        await().until(() -> cluster.allKVRangeIds().size() == 2);

        KVRangeConfig r0 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(0));
        KVRangeConfig r1 = cluster.kvRangeSetting(cluster.allKVRangeIds().get(1));
        AtomicReference<KVRangeConfig> merger;
        AtomicReference<KVRangeConfig> mergee;
        if (r0.boundary.hasEndKey() && BoundaryUtil.compare(r0.boundary.getEndKey(), r1.boundary.getStartKey()) <= 0) {
            merger = new AtomicReference<>(r0);
            mergee = new AtomicReference<>(r1);
        } else {
            merger = new AtomicReference<>(r1);
            mergee = new AtomicReference<>(r0);
        }

        KVRangeConfig mBefore = cluster.kvRangeSetting(merger.get().id);
        KVRangeConfig meBefore = cluster.kvRangeSetting(mergee.get().id);
        String mergerLeader = mBefore.leader;
        java.util.Iterator<String> it = followStores(mBefore).iterator();
        String f1 = it.next();
        String f2 = it.next();

        try (AutoCloseable dropSnapshotDataToFollowers = cluster.dropIf(m ->
            m.getPayload().hasSaveSnapshotDataRequest()
                && m.getPayload().getRangeId().equals(merger.get().id)
                && (m.getPayload().getHostStoreId().equals(f1) || m.getPayload().getHostStoreId().equals(f2)))) {

            cluster.merge(mergerLeader, mBefore.ver, mBefore.id, meBefore.id).toCompletableFuture().join();

            await().atMost(Duration.ofSeconds(30)).until(() -> {
                KVRangeDescriptor d1 = cluster.getKVRange(f1, merger.get().id);
                KVRangeDescriptor d2 = cluster.getKVRange(f2, merger.get().id);
                return d1 != null && d2 != null
                    && d1.getState() == State.StateType.PreparedMerging
                    && d2.getState() == State.StateType.PreparedMerging;
            });

            KVRangeStoreTestCluster.HoldHandle holdWalToFollowers = cluster.holdIf(m ->
                m.getPayload().hasWalRaftMessages()
                    && m.getPayload().getRangeId().equals(merger.get().id)
                    && (m.getPayload().getHostStoreId().equals(f1) || m.getPayload().getHostStoreId().equals(f2)));
            Thread.sleep(7000); // snapshotSyncIdleTimeoutSec=5
            // release wal, so leader could commit MergeDone
            holdWalToFollowers.close();
        }

        cluster.awaitKVRangeStateOnAllStores(merger.get().id, State.StateType.Normal, 120);
        Boundary expected = BoundaryUtil.combine(mBefore.boundary, meBefore.boundary);
        assertEquals(expected, cluster.kvRangeSetting(merger.get().id).boundary);
        for (String sid : cluster.allStoreIds()) {
            KVRangeDescriptor rd = cluster.getKVRange(sid, merger.get().id);
            if (rd != null) {
                assertEquals(rd.getState(), State.StateType.Normal);
                assertEquals(rd.getBoundary(), expected);
            }
        }
        cluster.awaitKVRangeStateOnAllStores(mergee.get().id, State.StateType.Merged, 60);
        for (String sid : cluster.allStoreIds()) {
            KVRangeDescriptor rd = cluster.getKVRange(sid, mergee.get().id);
            if (rd != null && rd.getState() == State.StateType.Merged) {
                assertEquals(rd.getBoundary(), NULL_BOUNDARY);
            }
        }
    }
}
