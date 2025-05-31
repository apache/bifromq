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

package org.apache.bifromq.basekv.raft;

import org.apache.bifromq.basekv.raft.event.CommitEvent;
import org.apache.bifromq.basekv.raft.event.ElectionEvent;
import org.apache.bifromq.basekv.raft.event.SnapshotRestoredEvent;
import org.apache.bifromq.basekv.raft.event.StatusChangedEvent;
import org.apache.bifromq.basekv.raft.exception.CompactionException;
import org.apache.bifromq.basekv.raft.exception.DropProposalException;
import org.apache.bifromq.basekv.raft.proto.AppendEntriesReply;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.RaftMessage;
import org.apache.bifromq.basekv.raft.proto.RequestPreVote;
import org.apache.bifromq.basekv.raft.proto.RequestPreVoteReply;
import org.apache.bifromq.basekv.raft.proto.RequestVoteReply;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.raft.proto.Voting;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.slf4j.Logger;

abstract class RaftNodeState implements IRaftNodeState {

    protected final String id;
    protected final RaftConfig config;
    protected final IRaftStateStore stateStorage;
    protected final Logger log;
    protected final IRaftNode.IRaftMessageSender sender;
    protected final IRaftNode.IRaftEventListener listener;
    protected final IRaftNode.ISnapshotInstaller snapshotInstaller;
    protected final OnSnapshotInstalled onSnapshotInstalled;
    protected final LinkedHashMap<Long, ProposeTask> uncommittedProposals;
    protected final int maxUncommittedProposals;
    protected final String[] tags;
    protected volatile long commitIndex;

    public RaftNodeState(
        long currentTerm,
        long commitIndex,
        RaftConfig config,
        IRaftStateStore stateStorage,
        LinkedHashMap<Long, ProposeTask> uncommittedProposals,
        IRaftNode.IRaftMessageSender sender,
        IRaftNode.IRaftEventListener listener,
        IRaftNode.ISnapshotInstaller installer,
        OnSnapshotInstalled onSnapshotInstalled,
        String... tags) {
        this.stateStorage = stateStorage;
        this.id = stateStorage.local();
        this.tags = tags;
        this.uncommittedProposals = uncommittedProposals;
        this.maxUncommittedProposals = config.getMaxUncommittedProposals() == 0 ? Integer.MAX_VALUE
            : config.getMaxUncommittedProposals();
        this.commitIndex = commitIndex;
        this.config = config;
        this.log = new RaftLogger(this, tags);
        this.sender = sender;
        this.listener = listener;
        this.snapshotInstaller = installer;
        this.onSnapshotInstalled = onSnapshotInstalled;
        if (currentTerm > currentTerm()) {
            this.stateStorage.saveTerm(currentTerm);
        }
    }

    @Override
    public final String id() {
        return id;
    }

    abstract RaftNodeState stepDown();

    abstract void recover(CompletableFuture<Void> onDone);

    abstract RaftNodeState tick();

    abstract void propose(ByteString fsmCmd, CompletableFuture<Long> onDone);

    abstract RaftNodeState stableTo(long stabledIndex);

    abstract RaftNodeState receive(String fromPeer, RaftMessage message);

    abstract void readIndex(CompletableFuture<Long> onDone);

    abstract void transferLeadership(String newLeader, CompletableFuture<Void> onDone);

    abstract void changeClusterConfig(String correlateId,
                                      Set<String> newVoters,
                                      Set<String> newLearners,
                                      CompletableFuture<Void> onDone);

    abstract void onSnapshotRestored(ByteString requested, ByteString installed, Throwable ex,
                                     CompletableFuture<Void> onDone);

    @Override
    public final long currentTerm() {
        return stateStorage.currentTerm();
    }

    @Override
    public long firstIndex() {
        return stateStorage.firstIndex();
    }

    @Override
    public long lastIndex() {
        return stateStorage.lastIndex();
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    final Optional<String> currentVote() {
        Optional<Voting> voting = stateStorage.currentVoting();
        return voting.map(Voting::getFor);
    }

    @Override
    public final ClusterConfig latestClusterConfig() {
        return stateStorage.latestClusterConfig();
    }

    @Override
    public void stop() {
        uncommittedProposals.forEach(
            (index, task) -> task.future.completeExceptionally(DropProposalException.cancelled()));
    }

    final ByteString latestSnapshot() {
        return stateStorage.latestSnapshot().getData();
    }

    final void retrieveCommitted(long fromIndex, long maxSize,
                                 CompletableFuture<Iterator<LogEntry>> onDone) {
        if (fromIndex < stateStorage.firstIndex() || fromIndex > stateStorage.lastIndex()) {
            onDone.completeExceptionally(new IndexOutOfBoundsException("Index out of range"));
        } else {
            onDone.complete(stateStorage.entries(fromIndex, commitIndex + 1, maxSize));
        }
    }

    final void entryAt(long index, CompletableFuture<Optional<LogEntry>> onDone) {
        onDone.complete(stateStorage.entryAt(index));
    }

    final void compact(ByteString fsmSnapshot, long compactIndex, CompletableFuture<Void> onDone) {
        Optional<LogEntry> compactEntry = stateStorage.entryAt(compactIndex);
        if (compactEntry.isPresent() || stateStorage.latestSnapshot().getIndex() == compactIndex) {
            // allow updating existing snapshot
            Snapshot newSnapshot = Snapshot.newBuilder()
                .setClusterConfig(stateStorage.latestClusterConfig())
                .setIndex(compactIndex)
                .setTerm(compactEntry.map(LogEntry::getTerm).orElse(stateStorage.latestSnapshot().getTerm()))
                .setData(fsmSnapshot)
                .build();
            try {
                long firstIndex = stateStorage.firstIndex();
                stateStorage.applySnapshot(newSnapshot);
                log.debug("Compacted entries[{},{}]", firstIndex, compactIndex);
                onDone.complete(null);
            } catch (Throwable e) {
                onDone.completeExceptionally(new CompactionException("Failed to apply snapshot", e));
            }
        } else {
            onDone.completeExceptionally(CompactionException.staleSnapshot());
        }
    }

    protected Set<String> voters() {
        ClusterConfig clusterConfig = stateStorage.latestClusterConfig();
        Set<String> voters = new HashSet<>(clusterConfig.getVotersList());
        voters.addAll(clusterConfig.getNextVotersList());
        return voters;
    }

    protected Set<String> remoteVoters() {
        ClusterConfig clusterConfig = stateStorage.latestClusterConfig();
        Set<String> all = (new HashSet<>(clusterConfig.getVotersList()));
        all.addAll(clusterConfig.getNextVotersList());
        all.remove(stateStorage.local());
        return all;
    }

    protected boolean promotable() {
        return voters().contains(id);
    }

    protected int randomizeElectionTimeoutTick() {
        return config.getElectionTimeoutTick() +
            ThreadLocalRandom.current().nextInt(1, config.getElectionTimeoutTick() + 1);
    }

    protected void submitRaftMessages(Map<String, List<RaftMessage>> messages) {
        Map<String, List<RaftMessage>> sendMessages = messages.entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!sendMessages.isEmpty()) {
            sender.send(sendMessages);
        }
    }

    protected void submitRaftMessages(String remotePeer, RaftMessage message) {
        submitRaftMessages(new HashMap<>() {{
            put(remotePeer, Collections.singletonList(message));
        }});
    }

    protected void submitRaftMessages(String remotePeer, List<RaftMessage> messages) {
        submitRaftMessages(new HashMap<>() {{
            put(remotePeer, messages);
        }});
    }

    protected void submitSnapshot(ByteString requested, String fromLeader) {
        snapshotInstaller.install(requested, fromLeader,
            (installed, ex) -> onSnapshotInstalled.done(requested, installed, ex));
    }

    protected void notifyCommit() {
        log.trace("Notify commit index[{}]", commitIndex);
        for (Iterator<Map.Entry<Long, ProposeTask>> it = uncommittedProposals.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, ProposeTask> entry = it.next();
            long proposalIndex = entry.getKey();
            ProposeTask task = entry.getValue();
            long proposalTerm = task.term;
            Optional<LogEntry> proposalEntry = stateStorage.entryAt(proposalIndex);
            if (proposalEntry.isPresent()) {
                // proposal still in logs
                if (proposalIndex <= commitIndex) {
                    // proposal may be committed
                    it.remove();
                    if (proposalTerm == proposalEntry.get().getTerm()) {
                        // proposal has committed
                        task.future.complete(proposalIndex);
                    } else {
                        // proposal has been overridden
                        task.future.completeExceptionally(DropProposalException.overridden());
                    }
                } else if (proposalTerm < currentTerm()) {
                    // current committed entry has newer term and less index, proposal will be overridden in the future
                    it.remove();
                    task.future.completeExceptionally(DropProposalException.overridden());
                } else {
                    // wait for commit index advancing
                    break;
                }
            } else {
                // proposal not in logs
                it.remove();
                task.future.completeExceptionally(DropProposalException.overridden());
            }
        }
        listener.onEvent(new CommitEvent(id, commitIndex));
    }

    protected void notifyLeaderElected(String leaderId, long term) {
        listener.onEvent(new ElectionEvent(id, leaderId, term));
    }

    protected void notifyStateChanged() {
        listener.onEvent(new StatusChangedEvent(id, getState()));
    }

    protected void notifySnapshotRestored() {
        listener.onEvent(new SnapshotRestoredEvent(id, stateStorage.latestSnapshot()));
    }

    protected boolean isUpToDate(long term, long index) {
        long localLastTerm = stateStorage.latestSnapshot().getTerm();
        long localLastIndex = stateStorage.latestSnapshot().getIndex();
        // if there are log entries
        if (stateStorage.lastIndex() >= stateStorage.firstIndex()) {
            localLastTerm = stateStorage.entryAt(stateStorage.lastIndex()).get().getTerm();
            localLastIndex = stateStorage.lastIndex();
        }
        return term > localLastTerm || (term == localLastTerm && index >= localLastIndex);
    }

    protected boolean isProposeThrottled() {
        return uncommittedProposals.size() > maxUncommittedProposals;
    }

    protected void handlePreVote(String fromPeer, long askedTerm, RequestPreVote request) {
        sendRequestPreVoteReply(fromPeer, askedTerm, isUpToDate(request.getLastLogTerm(), request.getLastLogIndex()));
    }

    protected void sendRequestPreVoteReply(String fromPeer, long term, boolean granted) {
        log.debug("Answering pre-vote request from peer[{}] of term[{}], granted?: {}", fromPeer, term, granted);
        RaftMessage reply = RaftMessage
            .newBuilder()
            .setTerm(term)
            .setRequestPreVoteReply(
                RequestPreVoteReply
                    .newBuilder()
                    .setVoteCouldGranted(granted)
                    .build())
            .build();
        submitRaftMessages(fromPeer, reply);
    }

    protected void sendRequestVoteReply(String fromPeer, long term, boolean granted) {
        RaftMessage requestVoteReply = RaftMessage
            .newBuilder()
            .setTerm(term)
            .setRequestVoteReply(
                RequestVoteReply
                    .newBuilder()
                    .setVoteGranted(granted)
                    .build())
            .build();
        submitRaftMessages(fromPeer, requestVoteReply);
    }

    protected void handleLowTermMessage(String fromPeer, RaftMessage message) {
        switch (message.getMessageTypeCase()) {
            case APPENDENTRIES, INSTALLSNAPSHOT -> {
                // probably an isolated server regains connectivity with higher term and receiving request from
                // leader of majority, we need to let the leader acknowledge the higher term and step down, so
                // that the partitioned member could rejoin and be stable
                log.debug("Reply to the leader[{}] of lower term[{}] to let it step down",
                    fromPeer, message.getTerm());
                submitRaftMessages(fromPeer,
                    RaftMessage.newBuilder()
                        // force leader step down and starts election in higher term
                        .setTerm(currentTerm())
                        .setAppendEntriesReply(
                            // it doesn't matter what type of the reply message is actually,
                            // the purpose is to step down the leader of lower term and ignored by
                            // follower.
                            AppendEntriesReply.getDefaultInstance()
                        )
                        .build());
            }
            case REQUESTPREVOTE -> {
                // let pre-vote candidate with lower term be aware of the higher term instead of dropping,
                // so that it had a chance to update its term, without this mechanism the cluster may deadlock when
                // pre-vote gradually enable using rolling restart.
                // for details checkout: https://github.com/etcd-io/etcd/issues/8501
                log.debug("Reject pre-vote from candidate[{}] of lower term[{}]", fromPeer, message.getTerm());
                submitRaftMessages(fromPeer, RaftMessage.newBuilder()
                    .setTerm(currentTerm())
                    .setRequestPreVoteReply(RequestPreVoteReply.newBuilder().setVoteCouldGranted(false).build())
                    .build());
            }
            default -> log.debug("Ignore message[{}] with lower term[{}] from peer[{}]",
                message.getMessageTypeCase(), message.getTerm(), fromPeer);

            // ignore other messages other than the leader issues
        }
    }

    interface OnSnapshotInstalled {
        CompletableFuture<Void> done(ByteString requested, ByteString installed, Throwable ex);
    }

    /**
     * The future of Uncommitted propose request.
     */
    protected static class ProposeTask {
        final long term;
        final CompletableFuture<Long> future;

        ProposeTask(long term, CompletableFuture<Long> future) {
            this.term = term;
            this.future = future;
        }
    }
}
