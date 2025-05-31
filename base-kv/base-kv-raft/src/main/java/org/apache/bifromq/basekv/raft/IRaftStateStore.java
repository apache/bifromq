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

import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.raft.proto.Voting;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Local Storage interface used by RAFT state machine to work with its local persistent state. The implementation MUST
 * provide the logical view of RAFT log like following :
 * <pre>
 * LogEntries |      Snapshot       | FirstIndex |    n    | LastIndex |
 * Term:      |          t          |  t or t+1  |   ...   | t or t+n  |
 * Index:     |          i          |    i+1     |   ...   |   i+n     |
 * Content:   | Config + AppSMSnap  |  Appdata   |   ...   |  Appdata  |
 * </pre>
 * So for newly initialized Raft State, it's application's responsibility to prepare the head-debut snapshot like: t =
 * 0, i = 0, Content = InitialConfig + Empty AppSMSnap
 *
 * <p>
 * IMPLEMENTOR NOTES: Raft StateMachine will call storage APIs synchronously in critical path, so try to optimize the
 * performance as much as possible
 */
public interface IRaftStateStore {
    /**
     * Identifier of the raft node.
     *
     * @return id of raft node
     */
    String local();

    /**
     * Get current log term.
     *
     * @return current log term
     */
    long currentTerm();

    /**
     * Save current log term.
     *
     * @param term current log term
     */
    void saveTerm(long term);

    /**
     * Retrieve voting in current log term if any.
     *
     * @return current voting
     */
    Optional<Voting> currentVoting();

    /**
     * Save voting in current log term.
     *
     * @param voting current voting
     */
    void saveVoting(Voting voting);

    /**
     * If non-compact part of raft log contains more than one cluster config log entries, returns the one with the
     * largest index(no matter whether is committed or not), otherwise returns the one in latest snapshot. Storage
     * implementation should consider maintaining a stack of "indexes" pointing to the positions of non-compact part of
     * raft log where cluster config is stored, to facilitate fast retrieval.
     *
     * @return latest cluster config
     */
    ClusterConfig latestClusterConfig();

    /**
     * Apply a snapshot, depending on the content in the snapshot, there are two cases may happen:
     * <br>
     * 1. snapshot represents a partial history(last log match), the log will be truncated until last index of the
     * snapshot
     * <br>
     * 2. snapshot represents a different history(last log mismatch), the local log will be discarded entirely
     * <br>
     * {@link #latestSnapshot() currentSnapshot} MUST return new snapshot after that.
     * <br>
     * NOTE: the clusterConfig in snapshot MUST include local raft node self
     *
     * @param snapshot snapshot to apply
     */
    void applySnapshot(Snapshot snapshot);

    /**
     * Retrieve latest snapshot for initialing Raft state machine, Its application's responsibility to prepare the
     * initial snapshot for bootstrapping a new raft node.
     *
     * @return latest snapshot
     */
    Snapshot latestSnapshot();

    /**
     * The index of log entry immediately follows latest snapshot, it always starts from 1, 0 is always representing the
     * last entry contained in snapshot, for initial snapshot it's a trivial dummy log entry with 0 term, 1 index and
     * empty data.
     *
     * @return the index of log entry immediately follows latest snapshot
     */
    long firstIndex();

    /**
     * The index of the last entry in the log. It will be 0 when there is no log entry For non-empty logs, invariant
     * firstIndex less than or equal lastIndex must be hold.
     *
     * @return the index of the last entry in the log
     */
    long lastIndex();

    /**
     * Retrieve the log entry at specified index.
     *
     * @param index the index of the log entry
     * @return the log entry at specified index
     */
    Optional<LogEntry> entryAt(long index);

    /**
     * Get an iterator for retrieving log entries between lo and hi(exclusively) and aggregated size no greater than
     * maxSize.
     *
     * @param lo the start index of the log entry
     * @param hi the end index of the log entry
     * @param maxSize the max size of the log entries
     * @return the iterator of the log entries
     */
    Iterator<LogEntry> entries(long lo, long hi, long maxSize);

    /**
     * Append log entries after specified index, if flush is true, registered StableListener must be called immediately
     * after the appended entries has been saved durably.
     *
     * @param entries entries to append, must not be empty
     * @param flush   if flush the log entry to persistent storage synchronously
     */
    void append(List<LogEntry> entries, boolean flush);

    /**
     * The listener will be called with the highest index of the log entry that guaranteed to be durable.
     *
     * @param listener the listener to be called
     */
    void addStableListener(StableListener listener);

    /**
     * This method will be called by RaftNode on stop, implementation should make unstable index stabilized, and should
     * NEVER call listener thereafter.
     */
    void stop();

    /**
     * The listener will be called with the highest index of the log entry that guaranteed to be durable.
     */
    interface StableListener {
        /**
         * The index of the persisted log entry.
         *
         * @param stabledIndex the index of the persisted log entry
         */
        void onStabilized(long stabledIndex);
    }
}
