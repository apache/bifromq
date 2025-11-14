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

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.base.util.AsyncRunner;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.basekv.proto.KVPair;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataReply;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataRequest;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class KVRangeDumpSession {
    private static final int MIN_CHUNK_BYTES = 128 * 1024;
    private static final int MAX_CHUNK_BYTES = 2 * 1024 * 1024;
    private static final double TARGET_ROUND_TRIP_NANOS = Duration.ofMillis(70).toNanos();
    private static final double EMA_ALPHA = 0.2d;
    private static final long PROGRESS_LOG_INTERVAL_NANOS = Duration.ofSeconds(5).toNanos();
    private final Logger log;
    private final String sessionId;
    private final KVRangeSnapshot snapshot;
    private final KVRangeId receiverRangeId;
    private final String receiverStoreId;
    private final IKVRangeMessenger messenger;
    private final ExecutorService executor;
    private final AsyncRunner runner;
    private final AtomicInteger reqId = new AtomicInteger();
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final Duration maxIdleDuration;
    private final CompletableFuture<Result> doneSignal = new CompletableFuture<>();
    private final DumpBytesRecorder recorder;
    private final SnapshotBandwidthGovernor bandwidthGovernor;
    private final long startDumpTS = System.nanoTime();
    private IKVRangeReader snapshotReader;
    private IKVIterator snapshotDataItr;
    private long totalEntries = 0;
    private long totalBytes = 0;
    private long lastSendTS;
    private long lastProgressLogTS = startDumpTS;
    private double buildTimeEwma = TARGET_ROUND_TRIP_NANOS;
    private double roundTripEwma = TARGET_ROUND_TRIP_NANOS;
    private int chunkHint;
    private volatile KVRangeMessage currentRequest;
    private volatile long lastReplyTS;

    KVRangeDumpSession(String sessionId,
                       KVRangeSnapshot snapshot,
                       KVRangeId receiverRangeId,
                       String receiverStoreId,
                       IKVRange accessor,
                       IKVRangeMessenger messenger,
                       Duration maxIdleDuration,
                       long bandwidth,
                       SnapshotBandwidthGovernor bandwidthGovernor,
                       DumpBytesRecorder recorder,
                       String... tags) {
        this.sessionId = sessionId;
        this.snapshot = snapshot;
        this.receiverRangeId = receiverRangeId;
        this.receiverStoreId = receiverStoreId;
        this.messenger = messenger;
        this.executor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("basekv-snapshot-dumper")),
            "mutator", "basekv.range", Tags.of(tags));
        this.runner = new AsyncRunner("basekv.runner.sessiondump", executor);
        this.maxIdleDuration = maxIdleDuration;
        this.recorder = recorder;
        this.bandwidthGovernor = bandwidthGovernor;
        this.chunkHint = initialChunkHint(bandwidth);
        this.log = MDCLogger.getLogger(KVRangeDumpSession.class, tags);
        if (!snapshot.hasCheckpointId()) {
            messenger.send(KVRangeMessage.newBuilder()
                .setRangeId(receiverRangeId)
                .setHostStoreId(receiverStoreId)
                .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setFlag(SaveSnapshotDataRequest.Flag.End)
                    .build())
                .build());
            executor.execute(() -> doneSignal.complete(Result.OK));
        } else if (!accessor.hasCheckpoint(snapshot)) {
            log.warn("No checkpoint found for snapshot: {}", snapshot);
            messenger.send(KVRangeMessage.newBuilder()
                .setRangeId(receiverRangeId)
                .setHostStoreId(receiverStoreId)
                .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setFlag(SaveSnapshotDataRequest.Flag.NotFound)
                    .build())
                .build());
            executor.execute(() -> doneSignal.complete(Result.NoCheckpoint));
        } else {
            snapshotReader = accessor.open(snapshot);
            snapshotDataItr = snapshotReader.iterator();
            snapshotDataItr.seekToFirst();
            Disposable disposable = messenger.receive()
                .mapOptional(m -> {
                    if (m.hasSaveSnapshotDataReply()) {
                        SaveSnapshotDataReply reply = m.getSaveSnapshotDataReply();
                        if (reply.getSessionId().equals(sessionId)) {
                            return Optional.of(reply);
                        }
                    }
                    return Optional.empty();
                })
                .observeOn(Schedulers.from(executor))
                .subscribe(this::handleReply);
            doneSignal.whenComplete((v, e) -> {
                snapshotDataItr.close();
                snapshotReader.close();
                disposable.dispose();
            });
            nextSaveRequest();
        }
    }

    String id() {
        return sessionId;
    }

    String checkpointId() {
        return snapshot.getCheckpointId();
    }

    void tick() {
        if (lastReplyTS == 0 || canceled.get()) {
            return;
        }
        long elapseNanos = Duration.ofNanos(System.nanoTime() - lastReplyTS).toNanos();
        if (maxIdleDuration.toNanos() < elapseNanos) {
            log.debug("DumpSession idle: session={}, follower={}", sessionId, receiverStoreId);
            cancel();
        } else if (maxIdleDuration.toNanos() / 2 < elapseNanos && currentRequest != null) {
            runner.add(() -> {
                if (maxIdleDuration.toNanos() / 2 < Duration.ofNanos(System.nanoTime() - lastReplyTS).toNanos()) {
                    messenger.send(currentRequest);
                }
            });
        }
    }

    void cancel() {
        if (canceled.compareAndSet(false, true)) {
            messenger.send(KVRangeMessage.newBuilder()
                .setRangeId(receiverRangeId)
                .setHostStoreId(receiverStoreId)
                .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                    .setSessionId(sessionId)
                    .setFlag(SaveSnapshotDataRequest.Flag.Error)
                    .build())
                .build());
            runner.add(() -> doneSignal.complete(Result.Canceled));
        }
    }

    CompletableFuture<Result> awaitDone() {
        return doneSignal.whenComplete((v, e) -> executor.shutdown());
    }

    private void handleReply(SaveSnapshotDataReply reply) {
        KVRangeMessage currReq = currentRequest;
        if (currReq == null) {
            return;
        }
        SaveSnapshotDataRequest req = currReq.getSaveSnapshotDataRequest();
        lastReplyTS = System.nanoTime();
        if (req.getReqId() == reply.getReqId()) {
            long ackLatency = lastSendTS > 0 ? lastReplyTS - lastSendTS : 0;
            if (ackLatency > 0) {
                roundTripEwma = ema(roundTripEwma, ackLatency);
            }
            currentRequest = null;
            switch (reply.getResult()) {
                case OK -> {
                    switch (req.getFlag()) {
                        case More -> nextSaveRequest();
                        case End -> runner.add(() -> doneSignal.complete(Result.OK));
                        default -> {
                            // do nothing
                        }
                    }
                }
                case NoSessionFound, Error -> runner.add(() -> doneSignal.complete(Result.Abort));
                default -> {
                    // do nothing
                }
            }
        }
    }

    private void nextSaveRequest() {
        runner.add(() -> {
            SaveSnapshotDataRequest.Builder reqBuilder = SaveSnapshotDataRequest.newBuilder()
                .setSessionId(sessionId)
                .setReqId(reqId.getAndIncrement());
            long buildStart = System.nanoTime();
            int dumpEntries = 0;
            int dumpBytes = 0;
            int maxChunkBytes = chunkHint;
            if (!canceled.get()) {
                try {
                    boolean firstKv = true;
                    while (!canceled.get()) {
                        if (!snapshotDataItr.isValid()) {
                            break;
                        }
                        ByteString key = snapshotDataItr.key();
                        ByteString value = snapshotDataItr.value();
                        int kvBytes = key.size() + value.size();
                        if (!firstKv && dumpBytes + kvBytes > maxChunkBytes) {
                            break;
                        }
                        reqBuilder.addKv(KVPair.newBuilder()
                            .setKey(key)
                            .setValue(value)
                            .build());
                        dumpBytes += kvBytes;
                        dumpEntries++;
                        firstKv = false;
                        snapshotDataItr.next();
                    }
                } catch (Throwable e) {
                    log.error("DumpSession error: session={}, follower={}", sessionId, receiverStoreId, e);
                    reqBuilder.clearKv();
                    reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.Error);
                }
            }
            if (canceled.get() && reqBuilder.getFlag() != SaveSnapshotDataRequest.Flag.Error) {
                log.debug("DumpSession has been canceled: session={}, follower={}", sessionId, receiverStoreId);
                reqBuilder.clearKv();
                reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.Error);
            }
            if (reqBuilder.getFlag() != SaveSnapshotDataRequest.Flag.Error) {
                if (dumpBytes == 0) {
                    if (!snapshotDataItr.isValid()) {
                        reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.End);
                    } else {
                        reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.More);
                    }
                } else {
                    reqBuilder.setFlag(snapshotDataItr.isValid()
                        ? SaveSnapshotDataRequest.Flag.More
                        : SaveSnapshotDataRequest.Flag.End);
                }
            }
            if (dumpBytes > 0 && reqBuilder.getFlag() != SaveSnapshotDataRequest.Flag.Error) {
                bandwidthGovernor.acquire(dumpBytes);
                long buildCost = System.nanoTime() - buildStart;
                adjustChunkHint(buildCost);
            }
            currentRequest = KVRangeMessage.newBuilder()
                .setRangeId(receiverRangeId)
                .setHostStoreId(receiverStoreId)
                .setSaveSnapshotDataRequest(reqBuilder.build())
                .build();
            long now = System.nanoTime();
            lastReplyTS = now;
            lastSendTS = now;
            recorder.record(dumpBytes);
            totalEntries += dumpEntries;
            totalBytes += dumpBytes;
            if (reqBuilder.getFlag() == SaveSnapshotDataRequest.Flag.End) {
                log.info(
                    "Dump snapshot completed: sessionId={}, follower={}, totalEntries={}, totalBytes={}, cost={}ms",
                    sessionId, receiverStoreId, totalEntries, totalBytes,
                    TimeUnit.NANOSECONDS.toMillis(now - startDumpTS));
            } else if (now - lastProgressLogTS >= PROGRESS_LOG_INTERVAL_NANOS) {
                log.info(
                    "Dump snapshot progress: sessionId={}, follower={}, totalEntries={}, totalBytes={}, elapsed={}ms",
                    sessionId, receiverStoreId, totalEntries, totalBytes,
                    TimeUnit.NANOSECONDS.toMillis(now - startDumpTS));
                lastProgressLogTS = now;
            }
            messenger.send(currentRequest);
            if (currentRequest.getSaveSnapshotDataRequest().getFlag() == SaveSnapshotDataRequest.Flag.Error) {
                doneSignal.complete(Result.Error);
            }
        });
    }

    private int initialChunkHint(long bandwidth) {
        if (bandwidth <= 0) {
            return MIN_CHUNK_BYTES * 2;
        }
        long suggested = bandwidth / 20;
        if (suggested <= 0) {
            suggested = MIN_CHUNK_BYTES;
        }
        return (int) Math.max(MIN_CHUNK_BYTES, Math.min(MAX_CHUNK_BYTES, suggested));
    }

    private void adjustChunkHint(long buildCostNanos) {
        buildTimeEwma = ema(buildTimeEwma, buildCostNanos);
        double dominant = Math.max(buildTimeEwma, roundTripEwma);
        int current = chunkHint;
        if (dominant < TARGET_ROUND_TRIP_NANOS / 2 && current < MAX_CHUNK_BYTES) {
            int increased = current + Math.max(MIN_CHUNK_BYTES / 4, (int) (current * 0.2));
            chunkHint = Math.min(MAX_CHUNK_BYTES, increased);
        } else if (dominant > TARGET_ROUND_TRIP_NANOS * 2 && current > MIN_CHUNK_BYTES) {
            chunkHint = Math.max(MIN_CHUNK_BYTES, current / 2);
        }
    }

    private double ema(double current, long sample) {
        return current + EMA_ALPHA * (sample - current);
    }

    enum Result {
        OK, NoCheckpoint, Canceled, Abort, Error
    }

    interface DumpBytesRecorder {
        void record(int bytes);
    }
}
