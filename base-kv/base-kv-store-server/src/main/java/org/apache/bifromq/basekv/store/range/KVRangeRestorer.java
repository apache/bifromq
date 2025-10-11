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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
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
        try {
            IKVRangeRestoreSession restoreSession = range.startRestore(rangeSnapshot, (count, bytes) ->
                log.info("Received snapshot data: session={}, leader={}, entries={}, bytes={}",
                    session.id, leader, count, bytes));
            log.info("Restoring from snapshot: session={}, leader={} \n{}", session.id, leader, rangeSnapshot);
            IKVRangeSnapshotReceiver receiver = new KVRangeSnapshotReceiver(session.id, rangeSnapshot.getId(), leader,
                messenger, metricManager, executor, idleTimeSec, log);
            CompletableFuture<IKVRangeSnapshotReceiver.Result> receiveFuture = receiver.start(restoreSession::put);
            onDone.whenComplete((v, e) -> {
                if (onDone.isCancelled()) {
                    restoreSession.abort();
                    receiveFuture.cancel(true);
                }
            });
            receiveFuture.whenCompleteAsync((result, e) -> {
                if (e != null) {
                    restoreSession.abort();
                    onDone.completeExceptionally(new KVRangeStoreException("Snapshot restore failed", e));
                    return;
                }
                if (result.code() == IKVRangeSnapshotReceiver.Code.DONE) {
                    restoreSession.done();
                    onDone.complete(null);
                    log.info("Restored from snapshot: session={}, leader={}, entries={}, bytes={}, cost={}ms",
                        session.id, session.leader, result.totalEntries(), result.totalBytes(),
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                } else {
                    restoreSession.abort();
                    onDone.completeExceptionally(new KVRangeStoreException("Snapshot restore failed: " + result));
                }
            }, executor);
            log.info("Send snapshot sync request to {} {}", leader, !onDone.isDone());
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

    private static class RestoreSession {
        final String id = UUID.randomUUID().toString();
        final KVRangeSnapshot snapshot;
        final CompletableFuture<Void> doneFuture = new CompletableFuture<>();
        final String leader;

        private RestoreSession(KVRangeSnapshot snapshot, String leader) {
            this.snapshot = snapshot;
            this.leader = leader;
        }
    }
}
