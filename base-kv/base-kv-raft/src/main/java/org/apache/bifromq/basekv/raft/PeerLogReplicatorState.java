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

package org.apache.bifromq.basekv.raft;

import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import org.slf4j.Logger;

abstract class PeerLogReplicatorState {
    protected final String peerId;
    protected final RaftConfig config;
    protected final IRaftStateStore stateStorage;
    protected final Logger logger;
    protected long matchIndex;
    protected long nextIndex;

    PeerLogReplicatorState(String peerId,
                           RaftConfig config,
                           IRaftStateStore stateStorage,
                           long matchIndex,
                           long nextIndex,
                           Logger logger) {
        this.peerId = peerId;
        this.config = config;
        this.stateStorage = stateStorage;
        this.matchIndex = matchIndex;
        this.nextIndex = nextIndex;
        this.logger = logger;
        logger.debug("Peer[{}] tracker[matchIndex:{},nextIndex:{},state:{}] initialized",
            peerId, matchIndex, nextIndex, state());
    }

    public final long matchIndex() {
        return matchIndex;
    }

    public final long nextIndex() {
        return nextIndex;
    }

    public abstract RaftNodeSyncState state();

    /**
     * an external clock signal to drive the state machine forward in case no other stimuli happens.
     */
    public abstract PeerLogReplicatorState tick();

    /**
     * the amount of matchIndex advanced per tick.
     *
     * @return the catchup rate
     */
    public abstract long catchupRate();

    /**
     * a flag indicating whether the append entries for given peer should be paused.
     *
     * @return true if the append entries should be paused
     */
    public abstract boolean pauseReplicating();

    /**
     * a flag indicating whether the given peer need a heartbeat due to heartbeatTimeoutTick exceed.
     *
     * @return true if peer need a heartbeat
     */
    public abstract boolean needHeartbeat();

    /**
     * backoff the next index when peer follower rejected the append entries request.
     *
     * @param peerRejectedIndex the index of mismatched log which is literally the prevLogIndex in appendEntries rpc
     * @param peerLastIndex     the index of last log entry in peer's raft log
     * @return the new state of the peer log replicator
     */
    public abstract PeerLogReplicatorState backoff(long peerRejectedIndex, long peerLastIndex);

    /**
     * update the match index when peer follower accepted the append entries request.
     *
     * @param peerLastIndex the index of last log entry in peer's raft log
     * @return the new state of the peer log replicator
     */
    public abstract PeerLogReplicatorState confirmMatch(long peerLastIndex);

    /**
     * advance the next index after sending log entries up to endIndex(inclusively) to follower.
     *
     * @param endIndex the index of last log entry to be sent
     * @return the new state of the peer log replicator
     */
    public abstract PeerLogReplicatorState replicateTo(long endIndex);
}
