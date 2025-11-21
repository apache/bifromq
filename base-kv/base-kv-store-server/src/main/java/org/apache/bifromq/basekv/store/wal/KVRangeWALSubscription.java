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

package org.apache.bifromq.basekv.store.wal;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.bifromq.base.util.AsyncRunner;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.raft.event.CommitEvent;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class KVRangeWALSubscription implements IKVRangeWALSubscription {
    private final Logger log;
    private final long maxFetchBytes;
    private final IKVRangeWAL wal;
    private final Executor executor;
    private final AsyncRunner fetchRunner;
    private final AsyncRunner applyRunner;
    private final IKVRangeWALSubscriber subscriber;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final AtomicBoolean fetching = new AtomicBoolean();
    private final AtomicBoolean restoring = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final CompletableFuture<Void> stopSign = new CompletableFuture<>();
    private final AtomicLong lastFetchedIdx = new AtomicLong();
    private final ConcurrentSkipListMap<Long, Boolean> pendingApplies = new ConcurrentSkipListMap<>();

    KVRangeWALSubscription(long maxFetchBytes,
                           IKVRangeWAL wal,
                           Observable<CommitEvent> commitIndex,
                           long lastFetchedIndex,
                           IKVRangeWALSubscriber subscriber,
                           Executor executor,
                           String... tags) {
        this.log = MDCLogger.getLogger(KVRangeWALSubscription.class, tags);
        this.maxFetchBytes = maxFetchBytes;
        this.wal = wal;
        this.executor = executor;
        this.fetchRunner = new AsyncRunner("basekv.runner.walfetch", executor,
            "rangeId", KVRangeIdUtil.toString(wal.rangeId()));
        this.applyRunner = new AsyncRunner("basekv.runner.fsmapply", executor,
            "rangeId", KVRangeIdUtil.toString(wal.rangeId()));
        this.subscriber = subscriber;
        this.lastFetchedIdx.set(lastFetchedIndex);
        this.subscriber.onSubscribe(this);
        disposables.add(wal.snapshotRestoreTask()
            .subscribe(task -> {
                // snapshot restore work is preemptive
                applyRunner.cancelAll();
                fetchRunner.cancelAll();
                fetching.set(false);
                restoring.set(true);
                fetchRunner.add(() -> {
                    applyRunner.cancelAll();
                    return applyRunner.add(restore(task));
                }).handle((snap, e) -> fetchRunner.add(() -> {
                    if (e != null) {
                        log.error("Failed to restore from snapshot\n{}", task.snapshot, e);
                        return;
                    }
                    log.debug("Snapshot installed\n{}", snap);
                    lastFetchedIdx.set(snap.getLastAppliedIndex());
                    pendingApplies.clear();
                    restoring.set(false);
                    scheduleFetchWAL();
                }));
            }));
        disposables.add(commitIndex
            .subscribe(c -> fetchRunner.add(() -> {
                if (restoring.get()) {
                    return;
                }
                Map.Entry<Long, Boolean> prevCommitIdx = pendingApplies.floorEntry(c.index);
                if (prevCommitIdx != null && prevCommitIdx.getValue() == c.isLeader) {
                    pendingApplies.remove(prevCommitIdx.getKey());
                }
                pendingApplies.put(c.index, c.isLeader);
                scheduleFetchWAL();
            })));
    }

    @Override
    public CompletionStage<Void> stop() {
        if (stopped.compareAndSet(false, true)) {
            disposables.dispose();
            fetchRunner.cancelAll();
            applyRunner.cancelAll();
            CompletableFuture
                .allOf(fetchRunner.awaitDone().toCompletableFuture(), applyRunner.awaitDone().toCompletableFuture())
                .exceptionally(e -> {
                    log.error("WAL Subscripiton stop error", e);
                    return null;
                })
                .whenComplete((v, e) -> stopSign.complete(null));
        }
        return stopSign;
    }

    private void scheduleFetchWAL() {
        if (!stopped.get() && fetching.compareAndSet(false, true)) {
            fetchRunner.add(this::fetchWAL);
        }
    }

    private CompletableFuture<Void> fetchWAL() {
        if (restoring.get()) {
            fetching.set(false);
            return CompletableFuture.completedFuture(null);
        }
        NavigableMap<Long, Boolean> toFetch = shouldFetch();
        if (!toFetch.isEmpty()) {
            return wal.retrieveCommitted(lastFetchedIdx.get() + 1, maxFetchBytes)
                .handleAsync((logEntries, e) -> {
                    if (e != null) {
                        log.error("Failed to retrieve log from wal from index[{}]", lastFetchedIdx.get() + 1, e);
                        fetching.set(false);
                        if (!(e instanceof IndexOutOfBoundsException)) {
                            scheduleFetchWAL();
                        }
                    } else {
                        fetchRunner.add(() -> {
                            LogEntry entry = null;
                            boolean hasMore = false;
                            while (logEntries.hasNext()) {
                                // no restore task interrupted
                                entry = logEntries.next();
                                Map.Entry<Long, Boolean> commitIdx = toFetch.ceilingEntry(entry.getIndex());
                                if (commitIdx != null) {
                                    boolean isLeader = commitIdx.getValue();
                                    applyRunner.add(applyLog(entry, isLeader));
                                } else {
                                    // fetch beyond the observed commit index
                                    hasMore = true;
                                    break;
                                }
                            }
                            logEntries.close();
                            if (entry != null) {
                                if (hasMore) {
                                    lastFetchedIdx.set(Math.max(entry.getIndex() - 1, lastFetchedIdx.get()));
                                } else {
                                    lastFetchedIdx.set(Math.max(entry.getIndex(), lastFetchedIdx.get()));
                                }
                            }
                            fetching.set(false);
                            if (!shouldFetch().isEmpty()) {
                                scheduleFetchWAL();
                            }
                        });
                    }
                    return null;
                }, executor);
        } else {
            fetching.set(false);
            if (!shouldFetch().isEmpty()) {
                scheduleFetchWAL();
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private NavigableMap<Long, Boolean> shouldFetch() {
        pendingApplies.headMap(lastFetchedIdx.get(), true).clear();
        return pendingApplies.tailMap(lastFetchedIdx.get() + 1);
    }

    private Supplier<CompletableFuture<Void>> applyLog(LogEntry logEntry, boolean isLeader) {
        return () -> {
            CompletableFuture<Void> onDone = new CompletableFuture<>();
            CompletableFuture<Void> applyFuture = subscriber.apply(logEntry, isLeader);
            onDone.whenComplete((v, e) -> {
                if (onDone.isCancelled()) {
                    applyFuture.cancel(true);
                }
            });
            applyFuture.whenCompleteAsync((v, e) -> fetchRunner.add(() -> {
                // always examine state and submit application task sequentially
                if (!onDone.isCancelled()) {
                    if (e != null) {
                        // reapply
                        applyRunner.addFirst(applyLog(logEntry, isLeader));
                    }
                }
                onDone.complete(null);
            }), executor);
            return onDone;
        };
    }

    private Supplier<CompletableFuture<KVRangeSnapshot>> restore(IKVRangeWAL.RestoreSnapshotTask task) {
        return () -> {
            CompletableFuture<KVRangeSnapshot> onDone = new CompletableFuture<>();
            CompletableFuture<Void> restoreTask =
                subscriber.restore(task.snapshot, task.leader, (installed, ex) -> task.afterRestored(installed, ex)
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            // error from fsm or raft
                            onDone.completeExceptionally(e);
                        } else {
                            // after raft applied the installed snapshot, we can continue to fetch wal
                            onDone.complete(installed);
                        }
                    }));
            onDone.whenComplete((v, e) -> {
                if (onDone.isCancelled()) {
                    restoreTask.cancel(true);
                }
            });
            return onDone;
        };
    }
}
