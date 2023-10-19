/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basekv.store.wal;

import static com.baidu.bifromq.basekv.store.util.KVUtil.toByteString;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_CONFIG_ENTRY_INDEXES_BYTES;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_CURRENT_TERM_BYTES;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_CURRENT_VOTING_BYTES;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_LATEST_SNAPSHOT_BYTES;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_LOG_ENTRIES_INCAR;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_PREFIX_LOG_ENTRIES_BYTES;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.configEntriesKey;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.configEntriesKeyPrefixInfix;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.logEntriesKeyPrefixInfix;
import static com.baidu.bifromq.basekv.store.wal.KVRangeWALKeys.logEntryKey;
import static com.baidu.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.lang.String.format;

import com.baidu.bifromq.basekv.localengine.IKVSpace;
import com.baidu.bifromq.basekv.localengine.IKVSpaceIterator;
import com.baidu.bifromq.basekv.localengine.IKVSpaceWriter;
import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.raft.proto.ClusterConfig;
import com.baidu.bifromq.basekv.raft.proto.LogEntry;
import com.baidu.bifromq.basekv.raft.proto.Snapshot;
import com.baidu.bifromq.basekv.raft.proto.Voting;
import com.baidu.bifromq.basekv.store.exception.KVRangeStoreException;
import com.baidu.bifromq.basekv.store.util.KVUtil;
import com.baidu.bifromq.basekv.utils.KVRangeIdUtil;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class KVRangeWALStore implements IKVRangeWALStore {
    private static final StableListener DEFAULT_STABLE_LISTENER = stabledIndex -> {
    };
    private final String storeId;
    private final KVRangeId rangeId;
    private final IKVSpace kvSpace;
    private final TreeMap<Long, ClusterConfig> configEntryMap = Maps.newTreeMap();
    private final Deque<StabilizingIndex> stabilizingIndices = new ConcurrentLinkedDeque<>();
    private final Consumer<KVRangeWALStore> onDestroy;
    private long currentTerm = 0;
    private Voting currentVoting;
    private Snapshot latestSnapshot;
    private long lastIndex;
    private int logEntriesKeyInfix;
    private volatile StableListener stableListener = DEFAULT_STABLE_LISTENER;

    KVRangeWALStore(String storeId, KVRangeId rangeId, IKVSpace kvSpace, Consumer<KVRangeWALStore> onDestroy) {
        this.rangeId = rangeId;
        this.kvSpace = kvSpace;
        this.storeId = storeId;
        this.onDestroy = onDestroy;
        load();
    }

    @Override
    public String local() {
        return storeId;
    }

    @Override
    public Optional<Voting> currentVoting() {
        return Optional.ofNullable(currentVoting);
    }

    @Override
    public void saveVoting(Voting voting) {
        trace("Save voting: {}", voting);
        kvSpace.toWriter().put(KEY_CURRENT_VOTING_BYTES, voting.toByteString()).done();
        flush();
        currentVoting = voting;
    }

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public void saveTerm(long term) {
        trace("Save term: {}", term);
        kvSpace.toWriter().put(KEY_CURRENT_TERM_BYTES,
            unsafeWrap(ByteBuffer.allocate(Long.BYTES).putLong(term).array())).done();
        flush();
        currentTerm = term;
    }

    @Override
    public ClusterConfig latestClusterConfig() {
        if (configEntryMap.isEmpty()) {
            return latestSnapshot.getClusterConfig();
        } else {
            return configEntryMap.lastEntry().getValue();
        }
    }

    @Override
    public void applySnapshot(Snapshot snapshot) {
        long snapLastIndex = snapshot.getIndex();
        long snapLastTerm = snapshot.getTerm();
        Optional<LogEntry> lastEntryInSS = entryAt(snapLastIndex);
        log.debug("Compact logs using snapshot[term={}, index={}]: rangeId={}, storeId={}",
            snapLastTerm, snapLastIndex, KVRangeIdUtil.toString(rangeId), storeId);
        IKVSpaceWriter writer = kvSpace.toWriter();
        if ((lastEntryInSS.isPresent() && lastEntryInSS.get().getTerm() == snapLastTerm) ||
            (lastEntryInSS.isEmpty() && latestSnapshot != null && latestSnapshot.getIndex() == snapLastIndex &&
                latestSnapshot.getTerm() == snapLastTerm)) {
            // the snapshot represents partial history, it happens when compacting
            // save snapshot
            writer.put(KEY_LATEST_SNAPSHOT_BYTES, snapshot.toByteString());
            latestSnapshot = snapshot;
            lastIndex = Math.max(lastIndex, snapLastIndex);

            long truncateBeforeIndex = Math.min(lastIndex(), snapLastIndex) + 1;
            while (!configEntryMap.isEmpty()) {
                if (configEntryMap.firstKey() <= truncateBeforeIndex) {
                    configEntryMap.pollFirstEntry();
                } else {
                    break;
                }
            }
            log.trace("Truncating logs before index[{}]: rangeId={}, storeId={}",
                truncateBeforeIndex, KVRangeIdUtil.toString(rangeId), storeId);
            try (IKVSpaceIterator it = kvSpace.newIterator()) {
                // truncate log entry
                writer.clear(Boundary.newBuilder()
                    .setStartKey(logEntriesKeyPrefixInfix(0))
                    .setEndKey(logEntryKey(logEntriesKeyInfix, truncateBeforeIndex))
                    .build());
                // truncate config entry indexes
                for (it.seek(configEntriesKeyPrefixInfix(0));
                     it.isValid() &&
                         it.key().startsWith(KEY_CONFIG_ENTRY_INDEXES_BYTES) &&
                         it.value().asReadOnlyByteBuffer().getLong() < truncateBeforeIndex;
                     it.next()) {
                    writer.delete(it.key());
                }
                writer.done();
                // clear sub range will trigger compaction and implicit flush
//                flushNotifier.notifyFlush();
            } catch (Throwable e) {
                log.error("Unexpected error during truncating log: rangeId={}, storeId={}",
                    KVRangeIdUtil.toString(rangeId), storeId, e);
            } finally {
                log.debug("Logs truncated before index[{}]: rangeId={}, storeId={}",
                    truncateBeforeIndex, KVRangeIdUtil.toString(rangeId), storeId);
            }
        } else {
            // the snapshot represents a different history, it happens when installing snapshot from leader
            // save snapshot
            writer.put(KEY_LATEST_SNAPSHOT_BYTES, snapshot.toByteString());
            latestSnapshot = snapshot;
            lastIndex = latestSnapshot.getIndex();

            // update and save logEntriesKeyInfix
            int lastLogEntriesKeyInfix = logEntriesKeyInfix;
            writer.put(KEY_LOG_ENTRIES_INCAR, toByteString(logEntriesKeyInfix + 1));
            logEntriesKeyInfix = logEntriesKeyInfix + 1;

            configEntryMap.clear();

            try {
                log.trace("Truncating all logs: rangeId={}, storeId={}", KVRangeIdUtil.toString(rangeId), storeId);
                // clear entire logs
                writer.clear(Boundary.newBuilder()
                    .setStartKey(logEntriesKeyPrefixInfix(0))
                    .setEndKey(upperBound(logEntriesKeyPrefixInfix(lastLogEntriesKeyInfix)))
                    .build());
                writer.clear(Boundary.newBuilder()
                    .setStartKey(configEntriesKeyPrefixInfix(0))
                    .setEndKey(upperBound(configEntriesKeyPrefixInfix(lastLogEntriesKeyInfix)))
                    .build());
                writer.done();
                flush();
                log.debug("All logs of truncated: rangeId={}, storeId={}", KVRangeIdUtil.toString(rangeId), storeId);
                // all previous index is stable after flush
            } catch (Throwable e) {
                log.error("Log truncation failed: rangeId={}, storeId={}", KVRangeIdUtil.toString(rangeId), storeId, e);
            }
        }
    }

    @Override
    public Snapshot latestSnapshot() {
        return latestSnapshot;
    }

    @Override
    public long firstIndex() {
        return latestSnapshot.getIndex() + 1;
    }

    @Override
    public long lastIndex() {
        return lastIndex;
    }

    @Override
    public Optional<LogEntry> entryAt(long index) {
        if (index < firstIndex() || index > lastIndex()) {
            return Optional.empty();
        }
        try {
            ByteString data = kvSpace.get(logEntryKey(logEntriesKeyInfix, index)).get();
            return Optional.of(LogEntry.parseFrom(data));
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse log entry[index={}, rangeId={}]", index, KVRangeIdUtil.toString(rangeId), e);
            return Optional.empty();
        }
    }

    @Override
    public Iterator<LogEntry> entries(long lo, long hi, long maxSize) {
        if (lo < firstIndex()) {
            throw new IndexOutOfBoundsException("lo[" + lo + "] must not be less than firstIndex["
                + firstIndex() + "]");
        }
        if (hi > lastIndex() + 1) {
            throw new IndexOutOfBoundsException("hi[" + hi + "] must not be greater than lastIndex["
                + lastIndex() + "]");
        }
        if (maxSize < 0) {
            maxSize = Long.MAX_VALUE;
        }
        return new LogEntryIterator(lo, hi, maxSize);
    }

    @Override
    public void append(List<LogEntry> entries, boolean flush) {
        assert !entries.isEmpty();
        LogEntry startEntry = entries.get(0);
        if (lastIndex() >= firstIndex()) {
            if (firstIndex() > startEntry.getIndex() || lastIndex() + 1 < startEntry.getIndex()) {
                throw new IndexOutOfBoundsException(format("first index[%d] must be in [%d,%d]",
                    startEntry.getIndex(), firstIndex(), lastIndex() + 1));
            }
        } else {
            if (startEntry.getIndex() != firstIndex()) {
                throw new IndexOutOfBoundsException(format("log index must start from %d", firstIndex()));
            }
        }

        long afterIndex = entries.get(0).getIndex() - 1;
        IKVSpaceWriter writer = kvSpace.toWriter();
        while (!configEntryMap.isEmpty() && configEntryMap.lastKey() > afterIndex) {
            long removedIndex = configEntryMap.pollLastEntry().getKey();
            writer.delete(configEntriesKey(logEntriesKeyInfix, removedIndex));
        }
        for (LogEntry entry : entries) {
            if (entry.hasConfig()) {
                configEntryMap.put(entry.getIndex(), entry.getConfig());
                writer.insert(configEntriesKey(logEntriesKeyInfix, entry.getIndex()), toByteString(entry.getIndex()));
                // force flush if log entry contains config
                flush = true;
            }
            trace("Append log entry[index={}, term={}, type={}], flush? {}",
                entry.getIndex(), entry.getTerm(), entry.getTypeCase().name(), flush);
            writer.insert(logEntryKey(logEntriesKeyInfix, entry.getIndex()), entry.toByteString());
        }
        writer.done();
        lastIndex = entries.get(entries.size() - 1).getIndex();
        stabilizingIndices.add(new StabilizingIndex(lastIndex));
        if (flush) {
            flush();
        } else {
            asyncFlush();
        }
    }

    @Override
    public void addStableListener(StableListener listener) {
        stableListener = listener;
    }

    @Override
    public void stop() {
        log.debug("Stop range wal storage: rangeId={}, storeId={}", KVRangeIdUtil.toString(rangeId), storeId);
        stableListener = DEFAULT_STABLE_LISTENER;
    }

    @Override
    public void destroy() {
        log.debug("Destroy walStore: rangeId={}, storeId={} ", KVRangeIdUtil.toString(rangeId), storeId);
        kvSpace.destroy();
        onDestroy.accept(this);
    }

    @Override
    public long size() {
        return kvSpace.size();
    }

    private void onStable(long flushTime) {
        StabilizingIndex stabilizingIndex;
        while ((stabilizingIndex = stabilizingIndices.poll()) != null) {
            if (stabilizingIndex.ts < flushTime) {
                stableListener.onStabilized(stabilizingIndex.index);
            } else {
                // add it back to queue
                stabilizingIndices.addLast(stabilizingIndex);
                break;
            }
        }
        if (!stabilizingIndices.isEmpty()) {
            asyncFlush();
        }
    }

    private void flush() {
        try {
            long flushTime = kvSpace.flush().join();
            onStable(flushTime);
        } catch (Throwable e) {
            log.warn("Flush error, try again", e);
        }
    }

    private void asyncFlush() {
        kvSpace.flush()
            .whenComplete((ts, e) -> {
                if (e != null) {
                    log.warn("Flush error, try again", e);
                    asyncFlush();
                } else {
                    onStable(ts);
                }
            });
    }

    private void load() {
        loadLogEntryInfix();
        loadVoting();
        loadCurrentTerm();
        loadLatestSnapshot();
        loadConfigEntryIndexes();
        loadLastIndex();
        trace("New raft state storage loaded");
    }

    private void loadVoting() {
        try {
            Optional<ByteString> votingBytes = kvSpace.get(KEY_CURRENT_VOTING_BYTES);
            if (votingBytes.isPresent()) {
                currentVoting = Voting.parseFrom(votingBytes.get());
            }
        } catch (InvalidProtocolBufferException e) {
            throw new KVRangeStoreException("Failed to parse currentVoting", e);
        }
    }

    private void loadCurrentTerm() {
        currentTerm = kvSpace.get(KEY_CURRENT_TERM_BYTES).map(KVUtil::toLong).orElse(0L);
    }

    private void loadLatestSnapshot() {
        try {
            ByteString latestSnapshotBytes = kvSpace.get(KEY_LATEST_SNAPSHOT_BYTES).get();
            latestSnapshot = Snapshot.parseFrom(latestSnapshotBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new KVRangeStoreException("Failed to parse snapshot", e);
        }
    }

    private void loadConfigEntryIndexes() {
        try (IKVSpaceIterator it = kvSpace.newIterator()) {
            ByteString prefix = KEY_CONFIG_ENTRY_INDEXES_BYTES;
            for (it.seek(prefix); it.isValid() && it.key().startsWith(prefix); it.next()) {
                long configEntryIndex = it.value().asReadOnlyByteBuffer().getLong();
                configEntryMap.put(configEntryIndex, loadConfigEntry(configEntryIndex));
            }
        }
    }

    private ClusterConfig loadConfigEntry(long configEntryIndex) {
        try {
            LogEntry logEntry =
                LogEntry.parseFrom(kvSpace.get(logEntryKey(logEntriesKeyInfix, configEntryIndex)).get());
            assert logEntry.hasConfig();
            return logEntry.getConfig();
        } catch (InvalidProtocolBufferException e) {
            throw new KVRangeStoreException("Failed to parse", e);
        } catch (NoSuchElementException e) {
            log.error("Cluster config not found at index[{}]: kvRangeId={}, storeId={}",
                configEntryIndex, KVRangeIdUtil.toString(rangeId), storeId);
            throw new KVRangeStoreException("config not found", e);
        }
    }

    private void loadLastIndex() {
        try (IKVSpaceIterator it = kvSpace.newIterator()) {
            it.seekToLast();
            if (it.isValid() && it.key().startsWith(KEY_PREFIX_LOG_ENTRIES_BYTES)) {
                lastIndex = KVRangeWALKeys.parseLogIndex(it.key());
            } else {
                lastIndex = latestSnapshot.getIndex();
            }
        }
    }

    private void loadLogEntryInfix() {
        logEntriesKeyInfix = kvSpace.get(KEY_LOG_ENTRIES_INCAR)
            .map(KVUtil::toInt)
            .orElse(0);
    }

    private class LogEntryIterator implements Iterator<LogEntry> {
        private final long maxIndex;
        private final long maxSize;
        private long currentIndex;
        private long accumulatedSize;
        private final IKVSpaceIterator iterator;

        private LogEntryIterator(long startIndex, long endIndex, long maxSize) {
            this.currentIndex = startIndex;
            this.maxIndex = endIndex;
            this.maxSize = maxSize;
            this.iterator = kvSpace.newIterator(
                Boundary.newBuilder()
                    .setStartKey(logEntryKey(logEntriesKeyInfix, currentIndex))
                    .setEndKey(upperBound(logEntriesKeyPrefixInfix(logEntriesKeyInfix)))
                    .build());
            this.iterator.seek(logEntryKey(logEntriesKeyInfix, currentIndex));
        }

        @Override
        public boolean hasNext() {
            boolean has = currentIndex < maxIndex && accumulatedSize <= maxSize;
            if (!has) {
                // the iterator can be closed automically by Cleaner if not scanned to the end
                iterator.close();
            }
            return has;
        }

        @Override
        public LogEntry next() {
            if (!iterator.isValid()) {
                throw new NoSuchElementException();
            } else {
                try {
                    LogEntry entry = LogEntry.parseFrom(iterator.value());
                    accumulatedSize += entry.getData().size();
                    currentIndex++;
                    iterator.next();
                    return entry;
                } catch (InvalidProtocolBufferException e) {
                    throw new KVRangeStoreException("Log data corruption", e);
                }
            }
        }
    }

    private void trace(String msg, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(msg, args);
        }
    }

    private static class StabilizingIndex {
        final long ts = System.nanoTime();
        final long index;

        private StabilizingIndex(long index) {
            this.index = index;
        }
    }
}
