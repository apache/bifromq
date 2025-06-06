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

package org.apache.bifromq.basecrdt.store;

import static org.apache.bifromq.basecrdt.util.Formatter.print;
import static org.apache.bifromq.basecrdt.util.Formatter.toPrintable;
import static org.apache.bifromq.basecrdt.util.ProtoUtil.to;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDTInflater;
import org.apache.bifromq.basecrdt.store.proto.AckMessage;
import org.apache.bifromq.basecrdt.store.proto.DeltaMessage;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

final class AntiEntropy {
    private final Logger log;
    private final ICausalCRDTInflater<?, ?> crdtInflater;
    private final ByteString localAddr;
    private final ByteString neighborAddr;
    private final ScheduledExecutorService executor;
    private final Subject<NeighborMessage> neighborMessageSubject;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean canceled = new AtomicBoolean();
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final int maxEventsInDelta;
    private final Counter deltaMsgCounter;
    private final Counter deltaMsgBytesCounter;

    private volatile long lastInflationTs = 0;
    private long neighborVer; // the neighbor index's version
    private Map<ByteString, NavigableMap<Long, Long>> neighborLatticeIndex;
    private Map<ByteString, NavigableMap<Long, Long>> neighborHistoryIndex;

    private int resendCount = 0;
    private ScheduledFuture<?> resendTask = null;
    private long currentNeighborVer;
    private long currentInflationTs;
    private DeltaMessage currentDelta = null;

    AntiEntropy(String storeId,
                ByteString localAddr,
                ByteString neighborAddr,
                ICausalCRDTInflater<?, ?> crdtInflater,
                Subject<NeighborMessage> neighborMessageSubject,
                ScheduledExecutorService executor,
                int maxEventsInDelta,
                Counter deltaMsgCounter,
                Counter deltaMsgBytesCounter) {
        this.log = MDCLogger.getLogger(AntiEntropy.class, "store", storeId, "replica", print(crdtInflater.id()));
        this.crdtInflater = crdtInflater;
        this.localAddr = localAddr;
        this.neighborAddr = neighborAddr;
        this.neighborMessageSubject = neighborMessageSubject;
        this.executor = executor;
        this.maxEventsInDelta = maxEventsInDelta;
        this.deltaMsgCounter = deltaMsgCounter;
        this.deltaMsgBytesCounter = deltaMsgBytesCounter;
        disposable.add(crdtInflater.getCRDT().inflation().subscribe(d -> {
            lastInflationTs = System.nanoTime();
            scheduleRun();
        }));
        // schedule the first run
        scheduleRun();
    }

    void updateObservedNeighborHistory(long ver,
                                       Map<ByteString, NavigableMap<Long, Long>> latticeIndex,
                                       Map<ByteString, NavigableMap<Long, Long>> historyIndex) {
        if (canceled.get()) {
            return;
        }
        synchronized (this) {
            if (ver > this.neighborVer) {
                this.neighborVer = ver;
                neighborLatticeIndex = latticeIndex;
                neighborHistoryIndex = historyIndex;
                // try schedule a run
                scheduleRun();
            }
        }
    }

    void handleAck(AckMessage ack) {
        if (canceled.get() || !running.get()) {
            return;
        }
        synchronized (this) {
            if (!running.get() || currentDelta == null) {
                return;
            }
            if (ack.getSeqNo() != currentDelta.getSeqNo()) {
                return;
            }
            // currentDelta has been ack'ed
            currentDelta = null;
            if (resendTask != null) {
                resendTask.cancel(false);
            }
            if (ack.getVer() > neighborVer) {
                // got newer neighbor's history
                neighborVer = ack.getVer();
                neighborLatticeIndex = to(ack.getLatticeEventsList());
                neighborHistoryIndex = to(ack.getHistoryEventsList());
            }
            running.set(false);
            // if there are new inflation happened or probe success, restart the task
            if (currentNeighborVer == 0 || lastInflationTs != currentInflationTs) {
                scheduleRun();
            }
        }
    }

    void cancel() {
        if (canceled.compareAndSet(false, true)) {
            log.debug("Local[{}] cancel anti-entropy to neighbor[{}] ",
                toPrintable(localAddr), toPrintable(neighborAddr));
            disposable.dispose();
            canceled.set(true);
            synchronized (this) {
                currentDelta = null;
                if (resendTask != null) {
                    resendTask.cancel(false);
                }
            }
        }
    }

    private void scheduleRun() {
        if (canceled.get()) {
            return;
        }
        if (running.compareAndSet(false, true)) {
            log.debug("Local[{}] start anti-entropy to neighbor[{}]",
                toPrintable(localAddr), toPrintable(neighborAddr));
            executor.execute(this::run);
        }
    }

    private void run() {
        if (canceled.get()) {
            return;
        }
        synchronized (this) {
            currentNeighborVer = neighborVer;
            currentInflationTs = lastInflationTs;
            if (currentNeighborVer == 0) {
                // Probe the neighbor's history
                currentDelta = DeltaMessage.newBuilder()
                    .setSeqNo(HLC.INST.get())
                    .addAllLatticeEvents(to(crdtInflater.latticeEvents()))
                    .addAllHistoryEvents(to(crdtInflater.historyEvents()))
                    .setVer(HLC.INST.get())
                    .build();
                send(currentDelta);
            } else {
                // Calculate delta
                crdtInflater.delta(neighborLatticeIndex, neighborHistoryIndex, maxEventsInDelta)
                    .whenComplete((delta, e) -> {
                        synchronized (this) {
                            if (e != null) {
                                log.error("Local[{}] failed to calculate delta for neighbor[{}]",
                                    toPrintable(localAddr), toPrintable(neighborAddr), e);
                                running.set(false);
                                return;
                            }
                            if (delta.isPresent()) {
                                currentDelta = DeltaMessage.newBuilder()
                                    .setSeqNo(HLC.INST.get())
                                    .addAllReplacement(delta.get())
                                    .addAllLatticeEvents(to(crdtInflater.latticeEvents()))
                                    .addAllHistoryEvents(to(crdtInflater.historyEvents()))
                                    .setVer(HLC.INST.get())
                                    .build();
                                send(currentDelta);
                            } else {
                                currentDelta = null;
                                resendCount = 0;
                                running.set(false);
                                if (currentNeighborVer != neighborVer || currentInflationTs != lastInflationTs) {
                                    // there are new inflation happened or neighbor's index has been updated
                                    scheduleRun();
                                }
                            }
                        }
                    });
            }
        }
    }

    private void send(DeltaMessage deltaMessage) {
        log.trace("Local[{}] send delta to neighbor[{}]:\n{}",
            toPrintable(localAddr), toPrintable(neighborAddr), toPrintable(deltaMessage));
        neighborMessageSubject.onNext(new NeighborMessage(deltaMessage, neighborAddr));
        // Schedule timer task for resend
        scheduleResend(deltaMessage);
    }

    private void scheduleResend(DeltaMessage toResend) {
        if (canceled.get()) {
            return;
        }
        resendTask = executor.schedule(() -> resend(toResend), resendDelay(), TimeUnit.MILLISECONDS);
    }

    private void resend(DeltaMessage toResend) {
        if (canceled.get()) {
            return;
        }
        synchronized (this) {
            if (currentDelta == toResend) {
                log.trace("Local[{}] resend delta to neighbor[{}]:\n{}",
                    toPrintable(localAddr), toPrintable(neighborAddr), toPrintable(toResend));
                deltaMsgCounter.increment(1D);
                deltaMsgBytesCounter.increment(currentDelta.getSerializedSize());
                neighborMessageSubject.onNext(new NeighborMessage(currentDelta, neighborAddr));
                if (resendCount++ < 10) {
                    scheduleResend(toResend);
                } else {
                    log.debug("Local[{}] resend delta to neighbor[{}] exceed max resend count, try probing",
                        toPrintable(localAddr), toPrintable(neighborAddr));
                    // reset neighbor ver so that we can probe the neighbor's history
                    neighborVer = 0;
                    currentDelta = null;
                    resendTask = null;
                    resendCount = 0;
                    running.set(false);
                    scheduleRun();
                }
            }
        }
    }

    private long resendDelay() {
        return ThreadLocalRandom.current().nextLong(500, 2000) * (resendCount + 1);
    }
}
