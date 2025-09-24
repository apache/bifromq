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

import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basekv.raft.exception.ClusterConfigChangeException;
import org.apache.bifromq.basekv.raft.exception.DropProposalException;
import org.apache.bifromq.basekv.raft.exception.LeaderTransferException;
import org.apache.bifromq.basekv.raft.exception.ReadIndexException;
import org.apache.bifromq.basekv.raft.exception.RecoveryException;
import org.apache.bifromq.basekv.raft.exception.SnapshotException;
import org.apache.bifromq.basekv.raft.proto.AppendEntries;
import org.apache.bifromq.basekv.raft.proto.AppendEntriesReply;
import org.apache.bifromq.basekv.raft.proto.InstallSnapshot;
import org.apache.bifromq.basekv.raft.proto.InstallSnapshotReply;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.Propose;
import org.apache.bifromq.basekv.raft.proto.ProposeReply;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.raft.proto.RequestReadIndex;
import org.apache.bifromq.basekv.raft.proto.RequestReadIndexReply;
import org.apache.bifromq.basekv.raft.proto.RequestVote;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.raft.proto.Voting;

class RaftNodeStateFollower extends RaftNodeState {
    private final TreeMap<Long, StabilizingTask> stabilizingIndexes = new TreeMap<>(Long::compareTo);
    private final LinkedHashMap<Long, Set<Integer>> tickToReadRequestsMap;
    private final Map<Integer, CompletableFuture<Long>> idToReadRequestMap;
    private final LinkedHashMap<Long, Set<Integer>> tickToForwardedProposesMap;
    private final Map<Integer, CompletableFuture<Long>> idToForwardedProposeMap;
    private int randomElectionTimeoutTick;
    private long currentTick;
    private int electionElapsedTick;
    private String currentLeader; // leader in current term
    private InstallSnapshot currentISSRequest;
    private int forwardReqId = 0;

    RaftNodeStateFollower(long term,
                          long commitIndex,
                          String leader,
                          RaftConfig config,
                          IRaftStateStore stateStorage,
                          IRaftNode.IRaftMessageSender sender,
                          IRaftNode.IRaftEventListener listener,
                          IRaftNode.ISnapshotInstaller installer,
                          OnSnapshotInstalled onSnapshotInstalled,
                          String... tags) {
        this(term,
            commitIndex,
            leader,
            config,
            stateStorage,
            new LinkedHashMap<>(),
            sender,
            listener,
            installer,
            onSnapshotInstalled,
            tags);
    }

    RaftNodeStateFollower(long term,
                          long commitIndex,
                          String leader,
                          RaftConfig config,
                          IRaftStateStore stateStorage,
                          LinkedHashMap<Long, ProposeTask> uncommittedProposals,
                          IRaftNode.IRaftMessageSender sender,
                          IRaftNode.IRaftEventListener listener,
                          IRaftNode.ISnapshotInstaller installer,
                          OnSnapshotInstalled onSnapshotInstalled,
                          String... tags) {
        super(term,
            commitIndex,
            config,
            stateStorage,
            uncommittedProposals,
            sender,
            listener,
            installer,
            onSnapshotInstalled,
            tags);
        currentLeader = leader;
        randomElectionTimeoutTick = randomizeElectionTimeoutTick();
        tickToReadRequestsMap = new LinkedHashMap<>();
        idToReadRequestMap = new HashMap<>();
        tickToForwardedProposesMap = new LinkedHashMap<>();
        idToForwardedProposeMap = new HashMap<>();
    }

    @Override
    public RaftNodeStatus getState() {
        return RaftNodeStatus.Follower;
    }

    @Override
    public String currentLeader() {
        return currentLeader;
    }

    @Override
    RaftNodeState stepDown() {
        return this;
    }

    @Override
    void recover(CompletableFuture<Void> onDone) {
        onDone.completeExceptionally(RecoveryException.notLostQuorum());
    }

    @Override
    RaftNodeState tick() {
        currentTick++;
        electionElapsedTick++;
        for (Iterator<Map.Entry<Long, Set<Integer>>> it = tickToReadRequestsMap.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Long, Set<Integer>> entry = it.next();
            if (entry.getKey() + 2L * config.getHeartbeatTimeoutTick() < currentTick) {
                // pending elapsed ticks exceed two times heartbeatTimeout, doing cleanup
                it.remove();
                entry.getValue().forEach(pendingReadId -> {
                    CompletableFuture<Long> pendingOnDone = idToReadRequestMap.remove(pendingReadId);
                    if (pendingOnDone != null && !pendingOnDone.isDone()) {
                        // if not finished by requestReadIndexReply then abort it
                        log.debug("Aborted forwarded timed-out ReadIndex request[{}]", pendingReadId);
                        pendingOnDone.completeExceptionally(ReadIndexException.forwardTimeout());
                    }
                });
            } else {
                break;
            }
        }
        for (Iterator<Map.Entry<Long, Set<Integer>>> it = tickToForwardedProposesMap.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Long, Set<Integer>> entry = it.next();
            if (entry.getKey() + 2L * config.getHeartbeatTimeoutTick() < currentTick) {
                // pending elapsed ticks exceed two times heartbeatTimeout, doing cleanup
                it.remove();
                entry.getValue().forEach(pendingProposalId -> {
                    CompletableFuture<Long> pendingOnDone = idToForwardedProposeMap.remove(pendingProposalId);
                    if (pendingOnDone != null && !pendingOnDone.isDone()) {
                        // if not finished by proposeReply then abort it
                        log.debug("Aborted forwarded timed-out Propose request[{}]", pendingProposalId);
                        pendingOnDone.completeExceptionally(DropProposalException.forwardTimeout());
                    }
                });
            } else {
                break;
            }
        }
        if (electionElapsedTick >= randomElectionTimeoutTick) {
            electionElapsedTick = 0;
            abortPendingReadIndexRequests(ReadIndexException.forwardTimeout());
            abortPendingProposeRequests(DropProposalException.forwardTimeout());
            // transit to candidate only if no snapshot installation is in progress
            if (currentISSRequest == null) {
                log.debug("Transit to candidate due to election timeout[{}]", randomElectionTimeoutTick);
                return new RaftNodeStateCandidate(
                    currentTerm(),
                    commitIndex,
                    config,
                    stateStorage,
                    uncommittedProposals,
                    sender,
                    listener,
                    snapshotInstaller,
                    onSnapshotInstalled,
                    tags)
                    .campaign(config.isPreVote(), false);
            }
        }
        return this;
    }

    @Override
    void propose(ByteString fsmCmd, CompletableFuture<Long> onDone) {
        if (config.isDisableForwardProposal()) {
            log.debug("Forward proposal to leader is disabled");
            onDone.completeExceptionally(DropProposalException.leaderForwardDisabled());
            return;
        }
        if (currentLeader == null) {
            log.debug("Dropped proposal due to no leader elected in current term");
            onDone.completeExceptionally(DropProposalException.noLeader());
            return;
        }
        if (isProposeThrottled()) {
            log.debug("Dropped proposal due to log growing[uncommittedProposals:{}] "
                    + "exceeds threshold[maxUncommittedProposals:{}]",
                uncommittedProposals.size(), maxUncommittedProposals);
            onDone.completeExceptionally(DropProposalException.throttledByThreshold());
            return;
        }
        int forwardProposalId = nextForwardReqId();
        tickToForwardedProposesMap.compute(currentTick, (k, v) -> {
            if (v == null) {
                v = new HashSet<>();
            }
            v.add(forwardProposalId);
            return v;
        });
        idToForwardedProposeMap.put(forwardProposalId, onDone);
        submitRaftMessages(currentLeader, RaftMessage.newBuilder()
            .setTerm(currentTerm())
            .setPropose(Propose.newBuilder()
                .setId(forwardProposalId)
                .setCommand(fsmCmd)
                .build())
            .build());
    }

    @Override
    RaftNodeState stableTo(long stabledIndex) {
        // send append entries reply for stabilized requests
        Set<Long> toRemove = new HashSet<>();
        for (Long index : stabilizingIndexes.keySet()) {
            if (index <= stabledIndex) {
                StabilizingTask task = stabilizingIndexes.get(index);
                while (task.pendingReplyCount-- > 0) {
                    if (currentLeader != null) {
                        log.trace("Entries below index[{}] stabilized, reply to leader[{}]",
                            index, currentLeader);
                        submitRaftMessages(currentLeader, RaftMessage.newBuilder()
                            .setTerm(currentTerm())
                            .setAppendEntriesReply(
                                AppendEntriesReply.newBuilder()
                                    .setAccept(AppendEntriesReply.Accept.newBuilder()
                                        .setLastIndex(index)
                                        .build())
                                    .setReadIndex(task.readIndex)
                                    .build())
                            .build());
                    }
                }
                if (task.committed) {
                    commitIndex = index;
                    log.trace("Advanced commitIndex[{}]", commitIndex);
                    notifyCommit();
                }
                toRemove.add(index);
            } else {
                break;
            }
        }
        stabilizingIndexes.keySet().removeAll(toRemove);
        return this;
    }

    @Override
    void readIndex(CompletableFuture<Long> onDone) {
        if (currentLeader == null) {
            log.debug("Dropped ReadIndex forwarding due to no leader elected in current term");
            onDone.completeExceptionally(ReadIndexException.noLeader());
        } else {
            int forwardReadId = nextForwardReqId();
            tickToReadRequestsMap.compute(currentTick, (k, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(forwardReadId);
                return v;
            });
            idToReadRequestMap.put(forwardReadId, onDone);
            submitRaftMessages(currentLeader, RaftMessage.newBuilder()
                .setTerm(currentTerm())
                .setRequestReadIndex(RequestReadIndex.newBuilder()
                    .setId(forwardReadId)
                    .build())
                .build());
        }
    }

    @Override
    RaftNodeState receive(String fromPeer, RaftMessage message) {
        log.trace("Receive[{}] from {}", message, fromPeer);
        RaftNodeState nextState = this;
        if (message.getTerm() > currentTerm()) {
            switch (message.getMessageTypeCase()) {
                case REQUESTPREVOTE:
                    if (!inLease()) {
                        handlePreVote(fromPeer, message.getTerm(), message.getRequestPreVote());
                    } else {
                        sendRequestPreVoteReply(fromPeer, message.getTerm(), false);
                    }
                    return nextState;
                case REQUESTPREVOTEREPLY:
                    // ignore the higher term pre-vote reply which may be a delayed reply of previous pre-vote
                    return nextState;
                case REQUESTVOTE:
                    // prevent from being disrupted
                    boolean leaderTransfer = message.getRequestVote().getLeaderTransfer();
                    if (!leaderTransfer && inLease() && !voters().contains(fromPeer)) {
                        log.debug("Vote[{}] from candidate[{}] not granted, lease is not expired",
                            message.getTerm(), fromPeer);
                        sendRequestVoteReply(fromPeer, message.getTerm(), false);
                        return nextState;
                    }
                    // fallthrough
                default:
                    log.debug("Higher term[{}] message[{}] received from peer[{}]",
                        message.getTerm(), message.getMessageTypeCase(), fromPeer);
                    stateStorage.saveTerm(message.getTerm());
                    // clear the known leader from prev term, otherwise canGrant may report wrong result
                    currentLeader = null;
            }
        } else if (message.getTerm() < currentTerm()) {
            handleLowTermMessage(fromPeer, message);
            return nextState;
        }
        // term match
        switch (message.getMessageTypeCase()) {
            case APPENDENTRIES:
                electionElapsedTick = 0; // reset tick
                randomElectionTimeoutTick = randomizeElectionTimeoutTick();
                handleAppendEntries(fromPeer, message.getAppendEntries());
                break;
            case INSTALLSNAPSHOT:
                electionElapsedTick = 0; // reset tick
                randomElectionTimeoutTick = randomizeElectionTimeoutTick();
                handleSnapshot(fromPeer, message.getInstallSnapshot());
                break;
            case REQUESTPREVOTE:
                // reject the pre-vote
                sendRequestPreVoteReply(fromPeer, currentTerm(), false);
                break;
            case REQUESTVOTE:
                handleVote(fromPeer, message.getRequestVote());
                break;
            case REQUESTREADINDEXREPLY:
                handleRequestReadIndexReply(message.getRequestReadIndexReply());
                break;
            case PROPOSEREPLY:
                handleProposeReply(message.getProposeReply());
                break;
            case TIMEOUTNOW:
                nextState = handleTimeoutNow(fromPeer);
                break;
            default: {
                // ignore other messages
            }
        }
        return nextState;
    }

    @Override
    void transferLeadership(String newLeader, CompletableFuture<Void> onDone) {
        onDone.completeExceptionally(LeaderTransferException.notLeader());
    }

    @Override
    void changeClusterConfig(String correlateId,
                             Set<String> newVoters,
                             Set<String> newLearners,
                             CompletableFuture<Void> onDone) {
        // TODO: support leader forward
        onDone.completeExceptionally(ClusterConfigChangeException.notLeader());
    }

    @Override
    void onSnapshotRestored(ByteString requested, ByteString installed, Throwable ex, CompletableFuture<Void> onDone) {
        if (currentISSRequest == null) {
            log.debug("Snapshot installation request not found");
            onDone.completeExceptionally(SnapshotException.noSnapshot());
            return;
        }
        InstallSnapshot iss = currentISSRequest;
        Snapshot snapshot = iss.getSnapshot();
        if (snapshot.getData() != requested) {
            if (ex != null) {
                log.debug("Obsolete snapshot install failed", ex);
                onDone.completeExceptionally(ex);
            } else {
                log.debug("Obsolete snapshot installation");
                onDone.completeExceptionally(SnapshotException.obsolete());
            }
            return;
        }
        currentISSRequest = null;
        RaftMessage reply;
        if (ex != null) {
            log.warn("Snapshot[index:{},term:{}] rejected by FSM", snapshot.getIndex(), snapshot.getTerm(), ex);
            reply = RaftMessage.newBuilder()
                .setTerm(currentTerm())
                .setInstallSnapshotReply(
                    InstallSnapshotReply
                        .newBuilder()
                        .setRejected(true)
                        .setLastIndex(snapshot.getIndex())
                        .setReadIndex(iss.getReadIndex())
                        .build())
                .build();
            submitRaftMessages(iss.getLeaderId(), reply);
            onDone.completeExceptionally(ex);
        } else {
            log.info("Snapshot[index:{},term:{}] accepted by FSM", snapshot.getIndex(), snapshot.getTerm());
            try {
                // replace fsm snapshot data with the installed one
                snapshot = snapshot.toBuilder().setData(installed).build();
                stateStorage.applySnapshot(snapshot);
                // reset commitIndex to last index in snapshot
                commitIndex = snapshot.getIndex();
                reply = RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setInstallSnapshotReply(
                        InstallSnapshotReply
                            .newBuilder()
                            .setRejected(false)
                            .setLastIndex(snapshot.getIndex())
                            .setReadIndex(iss.getReadIndex())
                            .build())
                    .build();
                notifySnapshotRestored();
                submitRaftMessages(iss.getLeaderId(), reply);
                onDone.complete(null);
            } catch (Throwable e) {
                log.error("Failed to apply snapshot[index:{}, term:{}]", snapshot.getIndex(), snapshot.getTerm(), e);
                reply = RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setInstallSnapshotReply(
                        InstallSnapshotReply
                            .newBuilder()
                            .setRejected(true)
                            .setLastIndex(snapshot.getIndex())
                            .setReadIndex(iss.getReadIndex())
                            .build())
                    .build();
                submitRaftMessages(iss.getLeaderId(), reply);
                onDone.completeExceptionally(e);
            }
        }
    }

    @Override
    public void stop() {
        super.stop();
        abortPendingReadIndexRequests(ReadIndexException.cancelled());
        abortPendingProposeRequests(DropProposalException.cancelled());
    }

    private void handleAppendEntries(String fromLeader, AppendEntries appendEntries) {
        if (!appendEntries.getLeaderId().equals(currentLeader)) {
            log.debug("Leader[{}] of current term[{}] elected", appendEntries.getLeaderId(), currentTerm());
            currentLeader = appendEntries.getLeaderId();
        }
        if (!entryMatch(appendEntries.getPrevLogIndex(), appendEntries.getPrevLogTerm())) {
            // prevLogEntry mismatch, reject
            log.debug("Rejected {} entries from leader[{}] due to mismatched last entry[index:{},term:{}]",
                appendEntries.getEntriesCount(),
                fromLeader,
                appendEntries.getPrevLogIndex(),
                appendEntries.getPrevLogTerm());
            Optional<LogEntry> lastEntry = stateStorage.entryAt(stateStorage.lastIndex());
            long lastEntryTerm = lastEntry.map(LogEntry::getTerm)
                .orElseGet(() -> stateStorage.latestSnapshot().getTerm());
            submitRaftMessages(fromLeader, RaftMessage.newBuilder()
                .setTerm(currentTerm())
                .setAppendEntriesReply(
                    AppendEntriesReply.newBuilder()
                        .setReject(AppendEntriesReply.Reject.newBuilder()
                            .setLastIndex(stateStorage.lastIndex())
                            .setTerm(lastEntryTerm)
                            .setRejectedIndex(appendEntries.getPrevLogIndex())
                            .build())
                        .setReadIndex(appendEntries.getReadIndex())
                        .build())
                .build());
        } else {
            if (appendEntries.getEntriesCount() > 0) {
                // prevLogEntry match, filter out duplicated append entries requests
                long newLastIndex = appendEntries.getEntries(appendEntries.getEntriesCount() - 1).getIndex();
                log.debug("Append {} entries after entry[index:{},term:{}]",
                    appendEntries.getEntriesCount(),
                    appendEntries.getPrevLogIndex(),
                    appendEntries.getPrevLogTerm());
                stabilizingIndexes.compute(newLastIndex, (k, v) -> {
                    if (v == null) {
                        v = new StabilizingTask();
                    }
                    v.pendingReplyCount++;
                    v.readIndex = appendEntries.getReadIndex();
                    return v;
                });
                // the higher index tasks are obsolete
                stabilizingIndexes.tailMap(newLastIndex, false).clear();
                stateStorage.append(appendEntries.getEntriesList(), !config.isAsyncAppend());
            } else {
                // heartbeat, reply immediately
                submitRaftMessages(currentLeader, RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setAppendEntriesReply(
                        AppendEntriesReply.newBuilder()
                            .setAccept(AppendEntriesReply.Accept.newBuilder()
                                .setLastIndex(appendEntries.getPrevLogIndex())
                                .build())
                            .setReadIndex(appendEntries.getReadIndex())
                            .build())
                    .build());
            }
            long newCommitIndex = appendEntries.getCommitIndex();
            if (commitIndex < newCommitIndex) {
                if (stabilizingIndexes.isEmpty()) {
                    // all entries have been stabilized
                    if (newCommitIndex <= stateStorage.lastIndex()) {
                        log.trace("Advanced commitIndex[from:{},to:{}]", commitIndex, newCommitIndex);
                        commitIndex = newCommitIndex;
                        // report to application
                        notifyCommit();
                    } else {
                        // entries between lastIndex and newCommitIndex missing, probably because the channel between
                        // leader and follower is lossy.
                        // In this case add it to stabilizingIndexes
                        log.debug("Committed entries[from:{},to:{}] missing locally",
                            stateStorage.lastIndex(), newCommitIndex);
                        stabilizingIndexes.compute(stateStorage.lastIndex(), (k, v) -> {
                            if (v == null) {
                                v = new StabilizingTask();
                            }
                            v.committed = true; // commit after stabilized
                            return v;
                        });
                    }
                    return;
                }
                if (newCommitIndex < stabilizingIndexes.firstKey()) {
                    // if the new commitIndex has been stabilized locally, then advance local commitIndex directly
                    log.trace("Entries before index[{}] have stabilized, Advanced commitIndex[from:{},to:{}]",
                        stabilizingIndexes.firstKey(), commitIndex, newCommitIndex);
                    commitIndex = newCommitIndex;
                    // report to application
                    notifyCommit();
                } else {
                    if (newCommitIndex > stabilizingIndexes.lastKey()) {
                        // if the newCommitIndex is greater than the largest local stabilizing index
                        // add it to stabilizingIndexes and mark it has been committed
                        log.debug("Committed Entries[from:{},to:{}] missing locally",
                            stabilizingIndexes.lastKey(), newCommitIndex);
                    }
                    stabilizingIndexes.compute(stateStorage.lastIndex(), (k, v) -> {
                        if (v == null) {
                            v = new StabilizingTask();
                        }
                        v.committed = true; // notify commit after stabilized
                        return v;
                    });
                }
            }
        }
    }

    private void handleSnapshot(String fromLeader, InstallSnapshot installSnapshot) {
        if (!installSnapshot.getLeaderId().equals(currentLeader)) {
            log.debug("Leader[{}] of current term elected", installSnapshot.getLeaderId());
            currentLeader = installSnapshot.getLeaderId();
        }
        Snapshot snapshot = installSnapshot.getSnapshot();

        Snapshot latestSnapshot = stateStorage.latestSnapshot();
        long localIndex = latestSnapshot.getIndex();
        long localTerm = latestSnapshot.getTerm();
        if (commitIndex > localIndex) {
            Optional<LogEntry> committedEntry = stateStorage.entryAt(commitIndex);
            if (committedEntry.isPresent()) {
                localIndex = commitIndex;
                localTerm = committedEntry.get().getTerm();
            }
        }
        if (localTerm > snapshot.getTerm()
            || (localTerm == snapshot.getTerm() && localIndex > snapshot.getIndex())) {
            // ignore obsolete snapshot
            log.debug("Ignore snapshot[index:{},term:{}] from peer[{}] since local committed progress[index:{},term:{}] is not behind",
                snapshot.getIndex(), snapshot.getTerm(), fromLeader, localIndex, localTerm);
            return;
        }

        // deduplicate snapshot installation
        if (currentISSRequest == null || isNewer(currentISSRequest.getSnapshot(), snapshot)) {
            currentISSRequest = installSnapshot;
            submitSnapshot(snapshot.getData(), currentLeader);
            log.debug("Snapshot[index:{},term:{}] from peer[{}] submitted to FSM",
                snapshot.getIndex(), snapshot.getTerm(), fromLeader);
        } else {
            // if installation timeout, leader will start probing again, which may lead to send duplicated snapshot
            // the deduplication could also be implemented at leader side, which may be more clear purposed
            log.debug("Ignore duplicated Snapshot[index:{},term:{}] from peer[{}]",
                snapshot.getIndex(), snapshot.getTerm(), fromLeader);
        }
    }

    private boolean isNewer(Snapshot existing, Snapshot newSnapshot) {
        return existing.getTerm() < newSnapshot.getTerm()
            ||
            (existing.getIndex() < newSnapshot.getIndex() && existing.getTerm() == newSnapshot.getTerm())
            ||
            (existing.getIndex() == newSnapshot.getIndex() && existing.getTerm() == newSnapshot.getTerm()
                && !existing.getData().equals(newSnapshot.getData()));
    }

    private void handleVote(String fromPeer, RequestVote request) {
        boolean canGrantVote = canGrantVote(request.getCandidateId(), request.getLeaderTransfer());
        boolean isLogUpToDate = isUpToDate(request.getLastLogTerm(), request.getLastLogIndex());
        boolean vote = canGrantVote && isLogUpToDate;
        if (vote) {
            // persist the state
            stateStorage.saveVoting(Voting.newBuilder()
                .setTerm(currentTerm()).setFor(request.getCandidateId()).build());
            // reset election tick when grant a vote
            electionElapsedTick = 0;
        }
        log.debug("Vote for peer[{}] with last log[index={}, term={}]? {}, grant? {}, log up-to-date? {}",
            fromPeer, request.getLastLogIndex(), request.getLastLogTerm(), vote, canGrantVote, isLogUpToDate);
        sendRequestVoteReply(fromPeer, currentTerm(), vote);
    }

    private void handleRequestReadIndexReply(RequestReadIndexReply reply) {
        CompletableFuture<Long> pendingOnDone = idToReadRequestMap.get(reply.getId());
        if (pendingOnDone != null) {
            pendingOnDone.complete(reply.getReadIndex());
        }
    }

    private void handleProposeReply(ProposeReply reply) {
        CompletableFuture<Long> pendingOnDone = idToForwardedProposeMap.get(reply.getId());
        if (pendingOnDone != null) {
            switch (reply.getCode()) {
                case Success -> pendingOnDone.complete(reply.getLogIndex());
                case DropByLeaderTransferring -> pendingOnDone.completeExceptionally(
                    DropProposalException.transferringLeader());
                case DropByMaxUnappliedEntries -> pendingOnDone.completeExceptionally(
                    DropProposalException.throttledByThreshold());
                case DropByNoLeader -> pendingOnDone.completeExceptionally(DropProposalException.noLeader());
                case DropByForwardTimeout ->
                    pendingOnDone.completeExceptionally(DropProposalException.forwardTimeout());
                case DropByOverridden -> pendingOnDone.completeExceptionally(DropProposalException.overridden());
                case DropBySupersededBySnapshot ->
                    pendingOnDone.completeExceptionally(DropProposalException.superseded());
                case DropByLeaderForwardDisabled ->
                    pendingOnDone.completeExceptionally(DropProposalException.leaderForwardDisabled());
                default -> {
                    assert reply.getCode() == ProposeReply.Code.DropByCancel;
                    pendingOnDone.completeExceptionally(DropProposalException.cancelled());
                }
            }
        }
    }

    private RaftNodeState handleTimeoutNow(String fromLeader) {
        if (promotable()) {
            log.info("Transited to candidate now by request from current leader[{}]", fromLeader);
            abortPendingReadIndexRequests(ReadIndexException.cancelled());
            abortPendingProposeRequests(DropProposalException.cancelled());
            return new RaftNodeStateCandidate(
                currentTerm(),
                commitIndex,
                config,
                stateStorage,
                uncommittedProposals,
                sender,
                listener,
                snapshotInstaller,
                onSnapshotInstalled,
                tags
            ).campaign(config.isPreVote(), true);
        }
        return this;
    }

    private int nextForwardReqId() {
        return forwardReqId = (forwardReqId + 1) % Integer.MAX_VALUE;
    }

    private boolean inLease() {
        // if quorum check enabled in local, then check if it's keeping touch with a known leader
        // Note: the known 'leader' may be the leader of isolated minority if it doesn't enable checkQuorum
        return currentLeader != null && electionElapsedTick < config.getElectionTimeoutTick();
    }

    private boolean canGrantVote(String candidateId, boolean leaderTransfer) {
        Optional<Voting> vote = stateStorage.currentVoting();
        // repeated vote or no leader elected while have not voted yet
        return (vote.isPresent() && vote.get().getTerm() == currentTerm() && vote.get().getFor().equals(candidateId))
            || ((currentLeader == null || leaderTransfer)
            && (vote.isEmpty() || vote.get().getTerm() < currentTerm()));
    }

    private void abortPendingReadIndexRequests(ReadIndexException e) {
        for (Iterator<Map.Entry<Long, Set<Integer>>> it = tickToReadRequestsMap.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Long, Set<Integer>> entry = it.next();
            it.remove();
            entry.getValue().forEach(pendingReadId -> {
                CompletableFuture<Long> pendingOnDone = idToReadRequestMap.remove(pendingReadId);
                if (pendingOnDone != null && !pendingOnDone.isDone()) {
                    // if not finished by requestReadIndexReply then abort it
                    pendingOnDone.completeExceptionally(e);
                }
            });
        }
    }

    private void abortPendingProposeRequests(DropProposalException e) {
        for (Iterator<Map.Entry<Long, Set<Integer>>> it = tickToForwardedProposesMap.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<Long, Set<Integer>> entry = it.next();
            it.remove();
            entry.getValue().forEach(pendingProposalId -> {
                CompletableFuture<Long> pendingOnDone = idToForwardedProposeMap.remove(pendingProposalId);
                if (pendingOnDone != null && !pendingOnDone.isDone()) {
                    // if not finished by requestReadIndexReply then abort it
                    pendingOnDone.completeExceptionally(e);
                }
            });
        }
    }

    private boolean entryMatch(long index, long term) {
        Optional<LogEntry> entry = stateStorage.entryAt(index);
        if (entry.isPresent()) {
            return entry.get().getTerm() == term;
        } else {
            Snapshot snapshot = stateStorage.latestSnapshot();
            return snapshot.getIndex() == index && snapshot.getTerm() == term;
        }
    }

    private static class StabilizingTask {
        int pendingReplyCount = 0;
        long readIndex = -1;
        boolean committed = false;
    }
}