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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.bifromq.basecrdt.core.api.ICRDTOperation;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDT;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDTInflater;
import org.apache.bifromq.basecrdt.store.proto.AckMessage;
import org.apache.bifromq.basecrdt.store.proto.DeltaMessage;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

/**
 * Manage a hosted replica's anti-entropy process with its neighbors.
 */
final class AntiEntropyManager {
    private final String storeId;
    private final Logger log;
    private final ByteString localAddr;
    private final ICausalCRDTInflater<?, ?> crdtInflater;
    private final Subject<NeighborMessage> neighborMessageSubject = PublishSubject.create();
    private final ScheduledExecutorService executor;
    private final Map<ByteString, AntiEntropy> neighborMap = Maps.newConcurrentMap(); // key: neighborAddr
    private final int maxEventsInDelta;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final MetricManager metricManager;

    public AntiEntropyManager(String storeId,
                              ByteString localAddr,
                              ICausalCRDTInflater<?, ?> crdtInflater,
                              ScheduledExecutorService executor,
                              int maxEventsInDelta,
                              String... tags) {
        this.storeId = storeId;
        this.localAddr = localAddr;
        this.log = MDCLogger.getLogger(AntiEntropyManager.class,
            "store", storeId, "replica", print(crdtInflater.id()));
        this.crdtInflater = crdtInflater;
        this.executor = executor;
        this.maxEventsInDelta = maxEventsInDelta;
        this.metricManager = new MetricManager(Tags.of(tags)
            .and("replica.uri", crdtInflater.id().getUri())
            .and("replica.id", BaseEncoding.base64().encode(crdtInflater.id().getId().toByteArray())));
    }

    ByteString localAddr() {
        return localAddr;
    }

    @SuppressWarnings("unchecked")
    <O extends ICRDTOperation, C extends ICausalCRDT<O>> C crdt() {
        return (C) crdtInflater.getCRDT();
    }

    Observable<NeighborMessage> neighborMessages() {
        return neighborMessageSubject;
    }

    CompletableFuture<AckMessage> receive(DeltaMessage delta, ByteString sender) {
        log.trace("Local[{}] receive delta[{}] from addr[{}]:\n{}",
            toPrintable(localAddr), delta.getSeqNo(), toPrintable(sender), toPrintable(delta));
        return handleDelta(delta, sender).thenApply(ack -> {
            metricManager.sendAckNum.increment();
            metricManager.sendAckBytes.increment(ack.getSerializedSize());
            log.trace("Local[{}] send ack[{}] to addr[{}]:\n{}",
                toPrintable(localAddr), ack.getSeqNo(), toPrintable(sender), toPrintable(ack));
            return ack;
        });
    }

    private CompletableFuture<AckMessage> handleDelta(DeltaMessage delta, ByteString sender) {
        metricManager.receiveDeltaNum.increment();
        metricManager.receiveDeltaBytes.increment(delta.getSerializedSize());
        AntiEntropy neighborAntiEntropy = neighborMap.get(sender);
        if (neighborAntiEntropy != null) {
            neighborAntiEntropy.updateObservedNeighborHistory(delta.getVer(), to(delta.getLatticeEventsList()),
                to(delta.getHistoryEventsList()));
        }
        if (delta.getReplacementList().isEmpty()) {
            return CompletableFuture.completedFuture(AckMessage.newBuilder()
                .setSeqNo(delta.getSeqNo())
                .addAllLatticeEvents(to(crdtInflater.latticeEvents()))
                .addAllHistoryEvents(to(crdtInflater.historyEvents()))
                .setVer(HLC.INST.get())
                .build());
        }
        return crdtInflater.join(delta.getReplacementList())
            .thenApply(v -> AckMessage.newBuilder()
                .setSeqNo(delta.getSeqNo())
                .addAllLatticeEvents(to(crdtInflater.latticeEvents()))
                .addAllHistoryEvents(to(crdtInflater.historyEvents()))
                .setVer(HLC.INST.get())
                .build());
    }

    void receive(AckMessage ack, ByteString neighborAddr) {
        metricManager.receiveAckNum.increment();
        metricManager.receiveAckBytes.increment(ack.getSerializedSize());
        AntiEntropy neighborAntiEntropy = neighborMap.get(neighborAddr);
        if (neighborAntiEntropy != null) {
            log.trace("Local[{}] receive ack[{}] from addr[{}]:\n{}",
                toPrintable(localAddr), ack.getSeqNo(), toPrintable(neighborAddr), toPrintable(ack));
            neighborAntiEntropy.handleAck(ack);
        } else {
            log.debug("Local[{}] ignore ack[{}] from addr[{}]:\n{}",
                toPrintable(localAddr), ack.getSeqNo(), toPrintable(neighborAddr), toPrintable(ack));
        }
    }

    void setNeighbors(Set<ByteString> neighborAddrs) {
        Set<ByteString> newNeighbors = Sets.newHashSet(Sets.difference(neighborAddrs, neighborMap.keySet()));
        Set<ByteString> delNeighbors = Sets.newHashSet(Sets.difference(neighborMap.keySet(), neighborAddrs));
        for (ByteString newNeighborAddr : newNeighbors) {
            log.trace("Local[{}] add new neighbor[{}]", toPrintable(localAddr), toPrintable(newNeighborAddr));
            neighborMap.put(newNeighborAddr, new AntiEntropy(
                storeId,
                localAddr,
                newNeighborAddr,
                crdtInflater,
                neighborMessageSubject,
                executor,
                maxEventsInDelta,
                metricManager.sendDeltaNum,
                metricManager.sendDeltaBytes)
            );
        }
        for (ByteString delNeighbor : delNeighbors) {
            log.trace("Local[{}] remove neighbor[{}]", toPrintable(localAddr), toPrintable(delNeighbor));
            neighborMap.remove(delNeighbor).cancel();
        }
    }

    CompletableFuture<Void> stop() {
        log.debug("Stop anti-entropy manager");
        neighborMap.values().forEach(AntiEntropy::cancel);
        neighborMessageSubject.onComplete();
        disposable.dispose();
        metricManager.close();
        return crdtInflater.stop();
    }

    private class MetricManager {
        private final Counter sendDeltaNum;
        private final Counter sendDeltaBytes;
        private final Counter receiveDeltaNum;
        private final Counter receiveDeltaBytes;
        private final Counter sendAckNum;
        private final Counter sendAckBytes;
        private final Counter receiveAckNum;
        private final Counter receiveAckBytes;
        private final Set<Meter> meters = new HashSet<>();

        MetricManager(Tags tags) {
            meters.add(Gauge.builder("basecrdt.neighbor", neighborMap, Map::size)
                .tags(tags)
                .register(Metrics.globalRegistry)
            );
            sendDeltaNum = Metrics.counter("basecrdt.send.delta.count", tags);
            sendDeltaBytes = Metrics.counter("basecrdt.send.delta.bytes", tags);
            receiveDeltaNum = Metrics.counter("basecrdt.receive.delta.count", tags);
            receiveDeltaBytes = Metrics.counter("basecrdt.receive.delta.bytes", tags);
            sendAckNum = Metrics.counter("basecrdt.send.ack.count", tags);
            sendAckBytes = Metrics.counter("basecrdt.send.ack.bytes", tags);
            receiveAckNum = Metrics.counter("basecrdt.receive.ack.count", tags);
            receiveAckBytes = Metrics.counter("basecrdt.receive.ack.bytes", tags);
        }

        void close() {
            meters.forEach(meter -> Metrics.globalRegistry.removeByPreFilterId(meter.getId()));
            Metrics.globalRegistry.removeByPreFilterId(sendAckNum.getId());
            Metrics.globalRegistry.removeByPreFilterId(sendAckBytes.getId());
            Metrics.globalRegistry.removeByPreFilterId(receiveAckNum.getId());
            Metrics.globalRegistry.removeByPreFilterId(receiveAckBytes.getId());
            Metrics.globalRegistry.removeByPreFilterId(sendDeltaNum.getId());
            Metrics.globalRegistry.removeByPreFilterId(sendDeltaBytes.getId());
            Metrics.globalRegistry.removeByPreFilterId(receiveDeltaNum.getId());
            Metrics.globalRegistry.removeByPreFilterId(receiveDeltaBytes.getId());
        }
    }
}
