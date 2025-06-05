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

import org.apache.bifromq.basekv.raft.event.RaftEvent;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Raft Node interface, which is used to interact with raft node.
 */
public interface IRaftNode {
    /**
     * Check if the raft node is started.
     *
     * @return true if the raft node is started, false otherwise
     */
    boolean isStarted();

    /**
     * the id of local raft node.
     *
     * @return the id of the node
     */
    String id();

    /**
     * role of the local raft node in current log term.
     *
     * @return the current status
     */
    RaftNodeStatus status();

    /**
     * tick from external clock which driving Raft StateMachine to move forward.
     */
    void tick();

    /**
     * Propose a app command. The returned future is guaranteed to be completed when the corresponding log entry is
     * COMMITTED or finished with exception indicating if the proposal MAYBE dropped due to timeout or other reason.
     *
     * @param appCommand the command to be proposed from application
     * @return the future of the proposal
     */
    CompletableFuture<Long> propose(ByteString appCommand);

    /**
     * Request an index for safely linearized read. NOTE: The returned will be completed by raft execution thread.
     *
     * @return the future of the index
     */
    CompletableFuture<Long> readIndex();

    /**
     * Receive raft messages from other Peers and drive local Raft StateMachine to proceed.
     *
     * @param fromPeer the peer who sent the message
     * @param message  the message sent by the peer
     */
    void receive(String fromPeer, RaftMessage message);

    /**
     * Compact RAFT logs by removing all log entries below compactIndex(inclusive), using the snapshot from
     * application's state machine. The snapshot is expected to be as light-weight as possible, so it's better to
     * include only some kind of metadata about the complete snapshot data, like file location, url etc. It's
     * application's duty to make sure the snapshot including latest state by applying the log until compactIndex.
     *
     * <br>
     * NOTE: The returned will be completed by raft execution thread.
     *
     * @param fsmSnapshot  the snapshot from application's state machine
     * @param compactIndex the index of the log entry to be compacted
     * @return the future of the compaction
     */
    CompletableFuture<Void> compact(ByteString fsmSnapshot, long compactIndex);

    /**
     * Transfer the leadership to new leader. NOTE: the returned future is completed as soon as the current leader is
     * STEP DOWN due to the transfer, NOT when the new leader elected. NOTE: The returned will be completed by raft
     * execution thread. Don't do make heavy work on it.
     *
     * @param newLeader the id of the new leader
     * @return the future of the transfer
     */
    CompletableFuture<Void> transferLeadership(String newLeader);

    /**
     * Recover if lost quorum because of majority members failed.
     *
     * @return the future of the recovery
     */
    CompletableFuture<Void> recover();

    /**
     * Returns the latest cluster config of the raft cluster.
     *
     * @return the latest cluster config
     */
    ClusterConfig latestClusterConfig();

    /**
     * If current node is leader, make it step down as a follower, which means an election is going to happen.
     *
     * @return true if it was a leader
     */
    Boolean stepDown();

    /**
     * Returns the latest snapshot in the raft node.
     *
     * @return the latest snapshot
     */
    ByteString latestSnapshot();

    /**
     * Change cluster membership config NOTE: The returned will be completed by raft execution thread.
     *
     * @param correlateId the id will be embedded in config entry, so caller could use it to associate with a real
     *                    request
     * @param voters      the new set of voters
     * @param learners    the new set of learners
     * @return the future of the config change
     */
    CompletableFuture<Void> changeClusterConfig(String correlateId, Set<String> voters, Set<String> learners);

    /**
     * Retrieve committed log entries from given index to current commitIndex. Note: maxSize controls max aggregated
     * size returned, so the returned may be fewer.
     *
     * @param fromIndex the start index of the log entry
     * @param maxSize   the max size of the log entries
     * @return the future of the log entries
     */
    CompletableFuture<Iterator<LogEntry>> retrieveCommitted(long fromIndex, long maxSize);

    /**
     * Start the Raft Node with necessary callbacks, this callbacks will be executed in raft's thread.
     *
     * @param sender    the message sender
     * @param listener  the event listener
     * @param installer the snapshot installer
     */
    void start(IRaftMessageSender sender, IRaftEventListener listener, ISnapshotInstaller installer);

    /**
     * Stop the raft node asynchronously.
     */
    CompletableFuture<Void> stop();

    /**
     * A callback interface for receiving raft events. The callback will be executed in raft thread, so it should be
     * non-blocking and fast.
     */
    interface IRaftEventListener {
        /**
         * This callback method will be executed in raft thread.
         *
         * @param event the raft event
         */
        void onEvent(RaftEvent event);
    }

    /**
     * A callback interface for sending raft messages to other peers. The callback will be executed in raft thread,
     * so it should be non-blocking and fast.
     */
    interface IRaftMessageSender {
        /**
         * This callback method will be executed in raft thread.
         *
         * @param messages the raft messages to be sent.
         */
        void send(Map<String, List<RaftMessage>> messages);
    }

    /**
     * Callback that Application must invoke after snapshot installation either successfully or failed. The returned
     * future will be fulfilled when raft finish applying the result snapshot, and is used to synchronize following work
     * at application side after the whole installation process has finished.
     */
    interface IAfterInstalledCallback {
        /**
         * Application must call this method when the snapshot installation is done.
         *
         * @param snapshot the snapshot of FSM installed.
         * @param ex      the exception if installation failed, null if installation succeeded.
         *
         * @return the future of the callback which will be completed when raft finish compacting the logs.
         */
        CompletableFuture<Void> call(ByteString snapshot, Throwable ex);
    }

    /**
     * Application specific snapshot installer, which is responsible for restoring application's state to given
     * snapshot.
     */
    interface ISnapshotInstaller {
        /**
         * Application specific async snapshot installation. Application must invoke the callback to notify the raft
         * node about the result.
         *
         * @param request  the snapshot requested to be installed
         * @param leader   the leader who sent the installation request
         * @param callback the promise to be completed when the installation is done
         */
        void install(ByteString request, String leader, IAfterInstalledCallback callback);
    }
}
