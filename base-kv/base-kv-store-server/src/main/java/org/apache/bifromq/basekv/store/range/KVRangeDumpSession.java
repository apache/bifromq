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

import com.google.common.util.concurrent.RateLimiter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.reactivex.rxjava3.disposables.Disposable;
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
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataReply;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataRequest;
import org.apache.bifromq.basekv.proto.SnapshotSyncRequest;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class KVRangeDumpSession {
    private final Logger log;
    private final String follower;
    private final SnapshotSyncRequest request;
    private final IKVRangeMessenger messenger;
    private final ExecutorService executor;
    private final AsyncRunner runner;
    private final AtomicInteger reqId = new AtomicInteger();
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final Duration maxIdleDuration;
    private final CompletableFuture<Result> doneSignal = new CompletableFuture<>();
    private final DumpBytesRecorder recorder;
    private final RateLimiter rateLimiter;
    private IKVCheckpointIterator snapshotDataItr;
    private volatile KVRangeMessage currentRequest;
    private volatile long lastReplyTS;

    KVRangeDumpSession(String follower,
                       SnapshotSyncRequest request,
                       IKVRange accessor,
                       IKVRangeMessenger messenger,
                       Duration maxIdleDuration,
                       long bandwidth,
                       DumpBytesRecorder recorder,
                       String... tags) {
        this.follower = follower;
        this.request = request;
        this.messenger = messenger;
        this.executor = ExecutorServiceMetrics.monitor(Metrics.globalRegistry,
            new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS, new LinkedTransferQueue<>(),
                EnvProvider.INSTANCE.newThreadFactory("basekv-snapshot-dumper")),
            "mutator", "basekv.range", Tags.of(tags));
        this.runner = new AsyncRunner("basekv.runner.sessiondump", executor);
        this.maxIdleDuration = maxIdleDuration;
        this.recorder = recorder;
        rateLimiter = RateLimiter.create(bandwidth);
        this.log = MDCLogger.getLogger(KVRangeDumpSession.class, tags);
        if (!request.getSnapshot().hasCheckpointId()) {
            messenger.send(KVRangeMessage.newBuilder()
                .setRangeId(request.getSnapshot().getId())
                .setHostStoreId(follower)
                .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                    .setSessionId(request.getSessionId())
                    .setFlag(SaveSnapshotDataRequest.Flag.End)
                    .build())
                .build());
            executor.execute(() -> doneSignal.complete(Result.OK));
        } else if (!accessor.hasCheckpoint(request.getSnapshot())) {
            log.warn("No checkpoint found for snapshot: {}", request.getSnapshot());
            messenger.send(KVRangeMessage.newBuilder()
                .setRangeId(request.getSnapshot().getId())
                .setHostStoreId(follower)
                .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                    .setSessionId(request.getSessionId())
                    .setFlag(SaveSnapshotDataRequest.Flag.Error)
                    .build())
                .build());
            executor.execute(() -> doneSignal.complete(Result.NoCheckpoint));
        } else {
            snapshotDataItr = accessor.open(request.getSnapshot()).newDataReader().iterator();
            snapshotDataItr.seekToFirst();
            Disposable disposable = messenger.receive()
                .mapOptional(m -> {
                    if (m.hasSaveSnapshotDataReply()) {
                        SaveSnapshotDataReply reply = m.getSaveSnapshotDataReply();
                        if (reply.getSessionId().equals(request.getSessionId())) {
                            return Optional.of(reply);
                        }
                    }
                    return Optional.empty();
                })
                .subscribe(this::handleReply);
            doneSignal.whenComplete((v, e) -> {
                snapshotDataItr.close();
                disposable.dispose();
            });
            nextSaveRequest();
        }
    }

    String id() {
        return request.getSessionId();
    }

    String checkpointId() {
        return request.getSnapshot().getCheckpointId();
    }

    void tick() {
        if (lastReplyTS == 0 || canceled.get()) {
            return;
        }
        long elapseNanos = Duration.ofNanos(System.nanoTime() - lastReplyTS).toNanos();
        if (maxIdleDuration.toNanos() < elapseNanos) {
            log.debug("DumpSession idle: session={}, follower={}", request.getSessionId(), follower);
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
        SaveSnapshotDataRequest req = currentRequest.getSaveSnapshotDataRequest();
        lastReplyTS = System.nanoTime();
        if (req.getReqId() == reply.getReqId()) {
            currentRequest = null;
            switch (reply.getResult()) {
                case OK -> {
                    switch (req.getFlag()) {
                        case More -> nextSaveRequest();
                        case End -> runner.add(() -> doneSignal.complete(Result.OK));
                    }
                }
                case NoSessionFound, Error -> runner.add(() -> doneSignal.complete(Result.Abort));
            }
        }
    }

    private void nextSaveRequest() {
        runner.add(() -> {
            SaveSnapshotDataRequest.Builder reqBuilder = SaveSnapshotDataRequest.newBuilder()
                .setSessionId(request.getSessionId())
                .setReqId(reqId.getAndIncrement());
            int dumpBytes = 0;
            while (true) {
                if (!canceled.get()) {
                    try {
                        if (snapshotDataItr.isValid()) {
                            KVPair kvPair = KVPair.newBuilder()
                                .setKey(snapshotDataItr.key())
                                .setValue(snapshotDataItr.value())
                                .build();
                            reqBuilder.addKv(kvPair);
                            int bytes = snapshotDataItr.key().size() + snapshotDataItr.value().size();
                            snapshotDataItr.next();
                            if (!rateLimiter.tryAcquire(bytes)) {
                                if (snapshotDataItr.isValid()) {
                                    reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.More);
                                } else {
                                    reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.End);
                                }
                                break;
                            }
                            dumpBytes += bytes;
                        } else {
                            // current iterator finished
                            reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.End);
                            break;
                        }
                    } catch (Throwable e) {
                        log.error("DumpSession error: session={}, follower={}",
                            request.getSessionId(), follower, e);
                        reqBuilder.clearKv();
                        reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.Error);
                        break;
                    }
                } else {
                    log.debug("DumpSession has been canceled: session={}, follower={}",
                        request.getSessionId(), follower);
                    reqBuilder.clearKv();
                    reqBuilder.setFlag(SaveSnapshotDataRequest.Flag.Error);
                    break;
                }
            }
            currentRequest = KVRangeMessage.newBuilder()
                .setRangeId(request.getSnapshot().getId())
                .setHostStoreId(follower)
                .setSaveSnapshotDataRequest(reqBuilder.build())
                .build();
            lastReplyTS = System.nanoTime();
            recorder.record(dumpBytes);
            messenger.send(currentRequest);
            if (currentRequest.getSaveSnapshotDataRequest().getFlag() == SaveSnapshotDataRequest.Flag.Error) {
                doneSignal.complete(Result.Error);
            }
        });
    }

    enum Result {
        OK, NoCheckpoint, Canceled, Abort, Error
    }

    interface DumpBytesRecorder {
        void record(int bytes);
    }
}
