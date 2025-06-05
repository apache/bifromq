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

import org.apache.bifromq.basekv.raft.event.SyncStateChangedEvent;
import org.apache.bifromq.basekv.raft.proto.RaftNodeSyncState;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;

class PeerLogTracker {
    private final String id;
    private final Map<String, IPeerLogReplicator> replicators = new HashMap<>();
    private final RaftConfig config;
    private final IRaftStateStore stateStorage;
    private final IRaftNode.IRaftEventListener listener;
    private final Logger logger;

    PeerLogTracker(
        String id,
        RaftConfig config,
        IRaftStateStore stateStorage,
        IRaftNode.IRaftEventListener listener,
        Logger logger) {
        this.id = id;
        this.config = config;
        this.stateStorage = stateStorage;
        this.listener = listener;
        this.logger = logger;
    }

    public void tick() {
        if (replicators.values().stream().anyMatch(IPeerLogReplicator::tick)) {
            notifyReplicationStatusChange();
        }
    }

    public void startTracking(Set<String> peerIds, boolean notify) {
        boolean changed = false;
        for (String peerId : peerIds) {
            if (replicators.putIfAbsent(peerId, new PeerLogReplicator(peerId, config, stateStorage, logger)) == null) {
                changed = true;
            }
        }
        if (changed && notify) {
            notifyReplicationStatusChange();
        }
    }

    public boolean isTracking(String peerId) {
        return replicators.containsKey(peerId);
    }

    public void stopTracking(Set<String> peerIds) {
        boolean notity = false;
        for (String peerId : peerIds) {
            if (replicators.remove(peerId) != null) {
                notity = true;
            }
        }
        if (notity) {
            notifyReplicationStatusChange();
        }
    }

    public void stopTracking(Predicate<String> predicate, boolean notify) {
        if (replicators.keySet().removeIf(predicate) && notify) {
            notifyReplicationStatusChange();
        }
    }

    /**
     * Current matching index.
     *
     * @return the match log index of the peer
     */
    public long matchIndex(String peerId) {
        return replicators.get(peerId).matchIndex();
    }

    /**
     * Next index to send.
     *
     * @return the next log index to send to the peer
     */
    public long nextIndex(String peerId) {
        return replicators.get(peerId).nextIndex();
    }

    /**
     * Current status.
     *
     * @return the current status
     */
    public RaftNodeSyncState status(String peerId) {
        return replicators.get(peerId).status();
    }

    /**
     * the amount of matchIndex advanced per tick always non-negative.
     *
     * @return the catchup rate per tick
     */
    public long catchupRate(String peerId) {
        return replicators.get(peerId).catchupRate();
    }

    /**
     * a flag indicating whether the append entries for given peer should be paused.
     *
     * @return true if the replicating should be paused
     */
    public boolean pauseReplicating(String peerId) {
        return replicators.get(peerId).pauseReplicating();
    }

    /**
     * a flag indicating whether the given peer need a heartbeat.
     *
     * @return true if peer need a heartbeat
     */
    public boolean needHeartbeat(String peerId) {
        return replicators.get(peerId).needHeartbeat();
    }

    /**
     * backoff the next index when peer follower rejected the append entries request.
     *
     * @param peerRejectedIndex the index of mismatched log which is literally the prevLogIndex in appendEntries rpc
     * @param peerLastIndex     the index of last log entry in peer's raft log
     */
    public void backoff(String peerId, long peerRejectedIndex, long peerLastIndex) {
        if (replicators.get(peerId).backoff(peerRejectedIndex, peerLastIndex)) {
            notifyReplicationStatusChange();
        }
    }

    /**
     * update the match index when peer follower accepted the append entries request.
     *
     * @param peerLastIndex the index of last log entry in peer's raft log
     */
    public void confirmMatch(String peerId, long peerLastIndex) {
        if (replicators.get(peerId).confirmMatch(peerLastIndex)) {
            notifyReplicationStatusChange();
        }
    }

    /**
     * advance the next index after sending log entries up to endIndex(inclusively) to follower.
     *
     * @param peerId   the peerId
     * @param endIndex the end log index
     */
    public void replicateBy(String peerId, long endIndex) {
        if (replicators.get(peerId).replicateBy(endIndex)) {
            notifyReplicationStatusChange();
        }
    }

    void notifyReplicationStatusChange() {
        Map<String, RaftNodeSyncState> replicationStatusMap = replicators
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().status()));
        listener.onEvent(new SyncStateChangedEvent(id, replicationStatusMap));
    }
}
