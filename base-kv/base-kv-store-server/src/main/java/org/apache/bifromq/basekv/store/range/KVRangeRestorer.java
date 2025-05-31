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

package org.apache.bifromq.basekv.store.range;

import org.apache.bifromq.basekv.proto.KVPair;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataReply;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataRequest;
import org.apache.bifromq.basekv.proto.SnapshotSyncRequest;
import org.apache.bifromq.basekv.store.exception.KVRangeStoreException;
import org.apache.bifromq.logger.SiftLogger;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.observers.DisposableObserver;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
        this.log = SiftLogger.getLogger(KVRangeRestorer.class, tags);
        RestoreSession initialSession = new RestoreSession(startSnapshot);
        initialSession.doneFuture.complete(null);
        currentSession.set(initialSession);
    }

    public CompletableFuture<Void> awaitDone() {
        return currentSession.get().doneFuture.exceptionally(ex -> null);
    }

    public CompletableFuture<Void> restoreFrom(String leader, KVRangeSnapshot rangeSnapshot) {
        RestoreSession session = new RestoreSession(rangeSnapshot);
        RestoreSession prevSession = currentSession.getAndSet(session);
        if (!prevSession.snapshot.equals(rangeSnapshot) && !prevSession.doneFuture.isDone()) {
            // cancel previous restore session
            log.info("Cancel previous restore session: session={} \n{}", prevSession.id, prevSession.snapshot);
            prevSession.doneFuture.cancel(true);
        }
        CompletableFuture<Void> onDone = session.doneFuture;
        try {
            IKVReseter restorer = range.toReseter(rangeSnapshot);
            log.info("Restoring from snapshot: session={}, leader={} \n{}", session.id, leader, rangeSnapshot);
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
                                    for (KVPair kv : request.getKvList()) {
                                        bytes += kv.getKey().size();
                                        bytes += kv.getValue().size();
                                        restorer.put(kv.getKey(), kv.getValue());
                                    }
                                    metricManager.reportRestore(bytes);
                                    log.debug("Saved {} bytes snapshot data, send reply to {}: session={}",
                                        bytes, m.getHostStoreId(), session.id);
                                    if (request.getFlag() == SaveSnapshotDataRequest.Flag.End) {
                                        if (!onDone.isCancelled()) {
                                            restorer.done();
                                            dispose();
                                            onDone.complete(null);
                                            log.info("Restored from snapshot: session={}", session.id);
                                        } else {
                                            restorer.abort();
                                            dispose();
                                            log.info("Snapshot restore canceled: session={}", session.id);
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
            log.debug("Send snapshot sync request to {} {}", leader, !onDone.isDone());
            if (!onDone.isDone()) {
                messenger.send(KVRangeMessage.newBuilder()
                    .setRangeId(range.id())
                    .setHostStoreId(leader)
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

        private RestoreSession(KVRangeSnapshot snapshot) {
            this.snapshot = snapshot;
        }
    }
}
