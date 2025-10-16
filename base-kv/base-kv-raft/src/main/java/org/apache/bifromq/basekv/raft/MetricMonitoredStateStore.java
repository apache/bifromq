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

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.List;
import java.util.Optional;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.raft.proto.Voting;

class MetricMonitoredStateStore implements IRaftStateStore {
    private final IRaftStateStore delegate;
    private final MetricManager metricManager;

    public MetricMonitoredStateStore(IRaftStateStore delegate, Tags tags) {
        this.delegate = delegate;
        this.metricManager = new MetricManager(tags);
    }

    @Override
    public String local() {
        return delegate.local();
    }

    @Override
    public long currentTerm() {
        return metricManager.currentTermTimer.record(delegate::currentTerm);
    }

    @Override
    public void saveTerm(long term) {
        metricManager.saveTermTimer.record(() -> delegate.saveTerm(term));
    }

    @Override
    public Optional<Voting> currentVoting() {
        return metricManager.currentVotingTimer.record(delegate::currentVoting);
    }

    @Override
    public void saveVoting(Voting voting) {
        metricManager.saveVotingTimer.record(() -> delegate.saveVoting(voting));
    }

    @Override
    public ClusterConfig latestClusterConfig() {
        return metricManager.latestClusterConfigTimer.record(delegate::latestClusterConfig);
    }

    @Override
    public void applySnapshot(Snapshot snapshot) {
        metricManager.applySnapshotTimer.record(() -> delegate.applySnapshot(snapshot));
    }

    @Override
    public Snapshot latestSnapshot() {
        return metricManager.latestSnapshotTimer.record(delegate::latestSnapshot);
    }

    @Override
    public long firstIndex() {
        return metricManager.firstIndexTimer.record(delegate::firstIndex);
    }

    @Override
    public long lastIndex() {
        return metricManager.lastIndexTimer.record(delegate::lastIndex);
    }

    @Override
    public Optional<LogEntry> entryAt(long index) {
        return metricManager.entryAtTimer.record(() -> delegate.entryAt(index));
    }

    @Override
    public ILogEntryIterator entries(long lo, long hi, long maxSize) {
        return metricManager.entriesTimer.record(() -> delegate.entries(lo, hi, maxSize));
    }

    @Override
    public void append(List<LogEntry> entries, boolean flush) {
        metricManager.appendTimer.record(() -> delegate.append(entries, flush));
    }

    @Override
    public void addStableListener(StableListener listener) {
        delegate.addStableListener(listener);
    }

    @Override
    public void stop() {
        delegate.stop();
        metricManager.close();
    }

    private static class MetricManager {
        final Timer currentTermTimer;

        final Timer saveTermTimer;

        final Timer currentVotingTimer;

        final Timer saveVotingTimer;

        final Timer latestClusterConfigTimer;

        final Timer applySnapshotTimer;

        final Timer latestSnapshotTimer;

        final Timer firstIndexTimer;

        final Timer lastIndexTimer;

        final Timer entryAtTimer;

        final Timer entriesTimer;

        final Timer appendTimer;

        MetricManager(Tags tags) {
            // timer for measuring currentTerm latency
            currentTermTimer = Metrics.timer("raft.store.currentterm", tags);

            // timer for measuring saveTerm latency
            saveTermTimer = Metrics.timer("raft.store.saveterm", tags);

            // timer for measuring currentVoting latency
            currentVotingTimer = Metrics.timer("raft.store.currentvoting", tags);

            // timer for measuring saveVoting latency
            saveVotingTimer = Metrics.timer("raft.store.savevoting", tags);

            // timer for measuring latestClusterConfig latency
            latestClusterConfigTimer = Metrics.timer("raft.store.latestclusterconfig", tags);

            // timer for measuring applySnapshot latency
            applySnapshotTimer = Metrics.timer("raft.store.applysnapshot", tags);

            // timer for measuring latestSnapshot latency
            latestSnapshotTimer = Metrics.timer("raft.store.snapshot", tags);

            // timer for measuring firstIndex latency
            firstIndexTimer = Metrics.timer("raft.store.firstindex", tags);

            // timer for measuring lastIndex latency
            lastIndexTimer = Metrics.timer("raft.store.lastindex", tags);

            // timer for measuring entryAt latency
            entryAtTimer = Metrics.timer("raft.store.entryat", tags);

            // timer for measuring entries latency
            entriesTimer = Metrics.timer("raft.store.entries", tags);

            // timer for measuring append latency
            appendTimer = Metrics.timer("raft.store.append", tags);
        }

        void close() {
            Metrics.globalRegistry.removeByPreFilterId(currentTermTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(saveTermTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(currentVotingTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(saveVotingTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(latestClusterConfigTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(applySnapshotTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(latestSnapshotTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(firstIndexTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(lastIndexTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(entryAtTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(entriesTimer.getId());

            Metrics.globalRegistry.removeByPreFilterId(appendTimer.getId());
        }
    }
}
