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

package org.apache.bifromq.basekv.store.range;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.observers.DisposableObserver;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basekv.proto.KVPair;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataReply;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataRequest;
import org.apache.bifromq.basekv.proto.SnapshotSyncRequest;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class KVRangeRestorer {
    private final Logger log;
    private final IKVRange range;
    private final IKVRangeMessenger messenger;
    private final IKVRangeMetricManager metricManager;
    private final Executor executor;
    private final int idleTimeSec;
    private final AdaptiveWriteBudget adaptiveWriteBudget;
    private final AtomicReference<RestoreSession> currentSession = new AtomicReference<>();

    KVRangeRestorer(KVRangeSnapshot startSnapshot,
                    IKVRange range,
                    IKVRangeMessenger messenger,
                    IKVRangeMetricManager metricManager,
                    Executor executor,
                    int idleTimeSec,
                    String... tags) {
        this.range = range;
        this.messenger = messenger;
        this.metricManager = metricManager;
        this.executor = executor;
        this.idleTimeSec = idleTimeSec;
        this.adaptiveWriteBudget = new AdaptiveWriteBudget();
        this.log = MDCLogger.getLogger(KVRangeRestorer.class, tags);
        RestoreSession initialSession = new RestoreSession(startSnapshot, null);
        initialSession.doneFuture.complete(null);
        currentSession.set(initialSession);
    }

    public CompletableFuture<Void> awaitDone() {
        return currentSession.get().doneFuture.exceptionally(ex -> null);
    }

    public CompletableFuture<Void> restoreFrom(String leader, KVRangeSnapshot rangeSnapshot) {
        RestoreSession existingSession = currentSession.get();
        if (existingSession != null
            && existingSession.snapshot.equals(rangeSnapshot)
            && !existingSession.doneFuture.isDone()
            && Objects.equals(existingSession.leader, leader)) {
            log.info("Reuse snapshot restore session: session={}, leader={} \n{}", existingSession.id,
                existingSession.leader, rangeSnapshot);
            return existingSession.doneFuture;
        }
        RestoreSession session = new RestoreSession(rangeSnapshot, leader);
        RestoreSession prevSession = currentSession.getAndSet(session);
        if (prevSession != null && !prevSession.doneFuture.isDone()) {
            // cancel previous restore session
            log.info("Cancel previous restore session: session={}, leader={} \n{}", prevSession.id,
                prevSession.leader, prevSession.snapshot);
            prevSession.doneFuture.cancel(true);
        }
        CompletableFuture<Void> onDone = session.doneFuture;
        long startNanos = System.nanoTime();
        AtomicLong totalEntries = new AtomicLong();
        AtomicLong totalBytes = new AtomicLong();
        try {
            IKVReseter restorer = range.toReseter(rangeSnapshot);
            log.info("Restoring from snapshot: session={}, leader={} \n{}", session.id, session.leader, rangeSnapshot);
            DisposableObserver<KVRangeMessage> observer = messenger.receive()
                .filter(m -> m.hasSaveSnapshotDataRequest()
                    && m.getSaveSnapshotDataRequest().getSessionId().equals(session.id))
                .timeout(idleTimeSec, TimeUnit.SECONDS)
                .observeOn(Schedulers.from(executor))
                .subscribeWith(new DisposableObserver<KVRangeMessage>() {
                    @Override
                    public void onNext(@NonNull KVRangeMessage m) {
                        SaveSnapshotDataRequest request = m.getSaveSnapshotDataRequest();
                        try {
                            switch (request.getFlag()) {
                                case More, End -> {
                                    int bytes = 0;
                                    int entries = 0;
                                    for (KVPair kv : request.getKvList()) {
                                        if (session.entries == 0 && session.bytes == 0) {
                                            session.batchStartNanos = System.nanoTime();
                                        }
                                        bytes += kv.getKey().size();
                                        bytes += kv.getValue().size();
                                        entries++;
                                        restorer.put(kv.getKey(), kv.getValue());
                                        session.entries++;
                                        session.bytes += kv.getKey().size() + kv.getValue().size();
                                        if (shouldRotate(session)) {
                                            flushSegment(restorer, session);
                                        }
                                    }
                                    if (request.getFlag() == SaveSnapshotDataRequest.Flag.More) {
                                        if (shouldRotate(session)) {
                                            flushSegment(restorer, session);
                                        }
                                    }
                                    metricManager.reportRestore(bytes);
                                    totalEntries.addAndGet(entries);
                                    totalBytes.addAndGet(bytes);
                                    if (request.getFlag() == SaveSnapshotDataRequest.Flag.End) {
                                        flushSegment(restorer, session);
                                        if (!onDone.isCancelled()) {
                                            restorer.done();
                                            dispose();
                                            onDone.complete(null);
                                            log.info(
                                                "Restored from snapshot: session={}, leader={}, entries={}, bytes={}, cost={}ms",
                                                session.id, session.leader, totalEntries.get(), totalBytes.get(),
                                                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                                        } else {
                                            restorer.abort();
                                            dispose();
                                            log.info("Snapshot restore canceled: session={}, leader={}",
                                                session.id, session.leader);
                                        }
                                    }
                                    messenger.send(KVRangeMessage.newBuilder()
                                        .setRangeId(range.id())
                                        .setHostStoreId(m.getHostStoreId())
                                        .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                                            .setReqId(request.getReqId())
                                            .setSessionId(request.getSessionId())
                                            .setResult(SaveSnapshotDataReply.Result.OK)
                                            .build())
                                        .build());
                                }
                                default -> throw new KVRangeStoreException("Snapshot dump failed");
                            }
                        } catch (Throwable t) {
                            log.error("Snapshot restored failed: session={}", session.id, t);
                            onError(t);
                            messenger.send(KVRangeMessage.newBuilder()
                                .setRangeId(range.id())
                                .setHostStoreId(m.getHostStoreId())
                                .setSaveSnapshotDataReply(SaveSnapshotDataReply.newBuilder()
                                    .setReqId(request.getReqId())
                                    .setSessionId(request.getSessionId())
                                    .setResult(SaveSnapshotDataReply.Result.Error)
                                    .build())
                                .build());
                        }
                    }

                    @Override
                    public void onError(@NonNull Throwable e) {
                        restorer.abort();
                        onDone.completeExceptionally(e);
                        dispose();
                    }

                    @Override
                    public void onComplete() {

                    }
                });
            onDone.whenComplete((v, e) -> {
                if (onDone.isCancelled()) {
                    observer.dispose();
                    restorer.abort();
                }
            });
            log.info("Send snapshot sync request: leader={}", session.leader);
            if (!onDone.isDone()) {
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(range.id())
                    .setHostStoreId(session.leader)
                    .setSnapshotSyncRequest(SnapshotSyncRequest.newBuilder()
                        .setSessionId(session.id)
                        .setSnapshot(rangeSnapshot)
                        .build())
                    .build());
            }
        } catch (Throwable t) {
            log.error("Unexpected error", t);
        }
        return onDone;
    }

    private boolean shouldRotate(RestoreSession session) {
        return adaptiveWriteBudget.shouldFlush(session.entries, session.bytes);
    }

    private void flushSegment(IKVReseter restorer, RestoreSession session) {
        if (session.entries > 0 || session.bytes > 0) {
            log.info("Flush snapshot data: sessionId={}, entries={}, bytes={}, leader={}", session.id,
                session.entries, session.bytes, session.leader);
            long entries = session.entries;
            long bytes = session.bytes;
            long batchStart = session.batchStartNanos > 0 ? session.batchStartNanos : System.nanoTime();
            restorer.flush();
            long latencyMillis = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStart));
            adaptiveWriteBudget.recordFlush(entries, bytes, latencyMillis);
            session.entries = 0;
            session.bytes = 0;
            session.batchStartNanos = -1;
        }
    }

    private static class RestoreSession {
        final String id = UUID.randomUUID().toString();
        final KVRangeSnapshot snapshot;
        final CompletableFuture<Void> doneFuture = new CompletableFuture<>();
        final String leader;
        long entries = 0;
        long bytes = 0;
        long batchStartNanos = -1;

        private RestoreSession(KVRangeSnapshot snapshot, String leader) {
            this.snapshot = snapshot;
            this.leader = leader;
        }
    }

}
