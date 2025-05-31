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

package org.apache.bifromq.basekv.store.wal;

import static org.apache.bifromq.basekv.store.util.KVUtil.toByteString;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_CONFIG_ENTRY_INDEXES_BYTES;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_CURRENT_TERM_BYTES;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_CURRENT_VOTING_BYTES;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_LATEST_SNAPSHOT_BYTES;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_LOG_ENTRIES_INCAR;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.KEY_PREFIX_LOG_ENTRIES_BYTES;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.configEntriesKey;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.configEntriesKeyPrefixInfix;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.logEntriesKeyPrefixInfix;
import static org.apache.bifromq.basekv.store.wal.KVRangeWALKeys.logEntryKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.lang.String.format;

import org.apache.bifromq.baseenv.ZeroCopyParser;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.raft.proto.Voting;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import org.apache.bifromq.basekv.store.util.KVUtil;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.logger.SiftLogger;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;
import org.slf4j.Logger;

class KVRangeWALStore implements IKVRangeWALStore {
    private static final StableListener DEFAULT_STABLE_LISTENER = stabledIndex -> {
    };
    private final Logger log;
    private final String storeId;
    private final KVRangeId rangeId;
    private final IWALableKVSpace kvSpace;
    private final TreeMap<Long, ClusterConfig> configEntryMap = Maps.newTreeMap();
    private final Deque<StabilizingIndex> stabilizingIndices = new ConcurrentLinkedDeque<>();
    private final Consumer<KVRangeWALStore> onDestroy;
    private final LogEntryIteratorPool logEntryIteratorPool;
    private long currentTerm = 0;
    private Voting currentVoting;
    private Snapshot latestSnapshot;
    private long lastIndex;
    private int logEntriesKeyInfix;
    private volatile StableListener stableListener = DEFAULT_STABLE_LISTENER;

    KVRangeWALStore(String clusterId, String storeId, KVRangeId rangeId, IWALableKVSpace kvSpace,
                    Consumer<KVRangeWALStore> onDestroy) {
        this.rangeId = rangeId;
        this.kvSpace = kvSpace;
        this.storeId = storeId;
        this.onDestroy = onDestroy;
        log = SiftLogger.getLogger(KVRangeWALStore.class, "clusterId", clusterId, "storeId", storeId, "rangeId",
            KVRangeIdUtil.toString(rangeId));
        logEntryIteratorPool = new LogEntryIteratorPool(kvSpace);
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
        log.debug("Compact logs using snapshot[term={}, index={}]", snapLastTerm, snapLastIndex);
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
            log.trace("Truncating logs before index[{}]", truncateBeforeIndex);
            try (IKVSpaceIterator it = kvSpace.newIterator()) {
                // truncate log entry
                writer.clear(Boundary.newBuilder()
                    .setStartKey(logEntriesKeyPrefixInfix(0))
                    .setEndKey(logEntryKey(logEntriesKeyInfix, truncateBeforeIndex))
                    .build());
                // truncate config entry indexes
                for (it.seek(configEntriesKeyPrefixInfix(0));
                     it.isValid()
                         && it.key().startsWith(KEY_CONFIG_ENTRY_INDEXES_BYTES)
                         && it.value().asReadOnlyByteBuffer().getLong() < truncateBeforeIndex;
                     it.next()) {
                    writer.delete(it.key());
                }
                writer.done();
                // clear sub range will trigger compaction and implicit flush
//                flushNotifier.notifyFlush();
            } catch (Throwable e) {
                log.error("Unexpected error during truncating log", e);
                throw e;
            } finally {
                log.debug("Logs truncated before index[{}]", truncateBeforeIndex);
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
                log.trace("Truncating all logs");
                // clear entire logs
                writer.clear(toBoundary(logEntriesKeyPrefixInfix(0),
                    upperBound(logEntriesKeyPrefixInfix(lastLogEntriesKeyInfix))));
                writer.clear(toBoundary(configEntriesKeyPrefixInfix(0),
                    upperBound(configEntriesKeyPrefixInfix(lastLogEntriesKeyInfix))));
                writer.done();
                flush();
                log.debug("All logs of truncated");
                // all previous index is stable after flush
            } catch (Throwable e) {
                log.error("Log truncation failed", e);
                throw e;
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
            return Optional.of(ZeroCopyParser.parse(data, LogEntry.parser()));
        } catch (Throwable e) {
            log.error("Failed to parse log entry[index={}]", index, e);
            return Optional.empty();
        }
    }

    @Override
    public Iterator<LogEntry> entries(long lo, long hi, long maxSize) {
        if (lo < firstIndex()) {
            throw new IndexOutOfBoundsException(
                "lo[" + lo + "] must not be less than firstIndex[" + firstIndex() + "]");
        }
        if (hi > lastIndex() + 1) {
            throw new IndexOutOfBoundsException(
                "hi[" + hi + "] must not be greater than lastIndex[" + lastIndex() + "]");
        }
        if (maxSize < 0) {
            maxSize = Long.MAX_VALUE;
        }
        return logEntryIteratorPool.acquire(lo, hi, maxSize, logEntriesKeyInfix);
    }

    @Override
    public void append(List<LogEntry> entries, boolean flush) {
        assert !entries.isEmpty();
        LogEntry startEntry = entries.get(0);
        if (lastIndex() >= firstIndex()) {
            if (firstIndex() > startEntry.getIndex() || lastIndex() + 1 < startEntry.getIndex()) {
                throw new IndexOutOfBoundsException(
                    format("first index[%d] must be in [%d,%d]", startEntry.getIndex(), firstIndex(), lastIndex() + 1));
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
            trace("Append log entry[index={}, term={}, type={}], flush? {}", entry.getIndex(), entry.getTerm(),
                entry.getTypeCase().name(), flush);
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
        log.debug("Stop WALStore");
        stableListener = DEFAULT_STABLE_LISTENER;
    }

    @Override
    public void destroy() {
        log.debug("Destroy WALStore");
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
                currentVoting = ZeroCopyParser.parse(votingBytes.get(), Voting.parser());
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
            latestSnapshot = ZeroCopyParser.parse(latestSnapshotBytes, Snapshot.parser());
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
            ByteString data = kvSpace.get(logEntryKey(logEntriesKeyInfix, configEntryIndex)).get();
            LogEntry logEntry = ZeroCopyParser.parse(data, LogEntry.parser());
            assert logEntry.hasConfig();
            return logEntry.getConfig();
        } catch (IOException e) {
            throw new KVRangeStoreException("Failed to parse", e);
        } catch (NoSuchElementException e) {
            log.error("Cluster config not found at index[{}]", configEntryIndex);
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
        logEntriesKeyInfix = kvSpace.get(KEY_LOG_ENTRIES_INCAR).map(KVUtil::toInt).orElse(0);
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
