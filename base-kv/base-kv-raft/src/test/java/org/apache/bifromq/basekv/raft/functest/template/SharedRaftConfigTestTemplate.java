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

package org.apache.bifromq.basekv.raft.functest.template;

import static org.apache.bifromq.basekv.raft.functest.RaftNodeGroup.RaftNodeTickMagnitude;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.raft.RaftConfig;
import org.apache.bifromq.basekv.raft.functest.RaftNodeGroup;
import org.apache.bifromq.basekv.raft.functest.annotation.Config;
import org.apache.bifromq.basekv.raft.functest.annotation.Ticker;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterMethod;

@Slf4j
public abstract class SharedRaftConfigTestTemplate extends RaftGroupTestTemplate {
    private final RaftConfig defaultRaftConfig = RaftNodeGroup.DefaultRaftConfig;
    protected RaftNodeGroup group;
    private RaftConfig raftConfigInUse;
    private boolean disableTick = false;
    private int tickInMS = 10;

    @Override
    protected void doSetup(Method method) {
        Config config = method.getAnnotation(Config.class);
        Ticker ticker = method.getAnnotation(Ticker.class);
        raftConfigInUse = config == null ? defaultRaftConfig : build(config);
        if (ticker != null) {
            tickInMS = ticker.unitInMS();
            disableTick = ticker.disable();
        } else {
            tickInMS = 10;
            disableTick = false;
        }

        log.info("Setup a test raft group: v={}, l={}, nv={}, nl={}",
                clusterConfig().getVotersList(),
                clusterConfig().getLearnersList(),
                clusterConfig().getNextVotersList(),
                clusterConfig().getNextLearnersList());
        group = new RaftNodeGroup(clusterConfig(), raftConfigInUse);
        if (!disableTick) {
            group.run(tickInMS, TimeUnit.MILLISECONDS);
            await().forever().until(() -> group.currentLeader().isPresent());
            String leader = group.currentLeader().get();
            assertTrue(group.awaitIndexCommitted(leader, 1));
            log.info("Leader {} elected", leader);
        }
    }

    @AfterMethod(alwaysRun = true)
    public final void teardown() {
        log.info("Stop the test raft group");
        group.shutdown();
        raftConfigInUse = null;
    }

    public final RaftConfig raftConfig() {
        return raftConfigInUse;
    }

    public final int ticks(int electionRound) {
        return electionRound * raftConfig().getElectionTimeoutTick() * RaftNodeTickMagnitude;
    }

    private RaftConfig build(Config c) {
        return new RaftConfig()
            .setAsyncAppend(c.asyncAppend())
            .setDisableForwardProposal(c.disableForwardProposal())
            .setElectionTimeoutTick(c.electionTimeoutTick())
            .setHeartbeatTimeoutTick(c.heartbeatTimeoutTick())
            .setInstallSnapshotTimeoutTick(c.installSnapshotTimeoutTick())
            .setMaxInflightAppends(c.maxInflightAppends())
            .setMaxSizePerAppend(c.maxSizePerAppend())
            .setPreVote(c.preVote())
            .setReadOnlyBatch(c.readOnlyBatch())
            .setReadOnlyLeaderLeaseMode(c.readOnlyLeaderLeaseMode());
    }
}
