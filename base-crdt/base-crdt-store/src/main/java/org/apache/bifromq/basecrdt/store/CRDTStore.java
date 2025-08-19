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

import static org.apache.bifromq.basecrdt.store.MessagePayloadUtil.compressToPayload;
import static org.apache.bifromq.basecrdt.store.MessagePayloadUtil.decompress;
import static org.apache.bifromq.basecrdt.util.Formatter.toPrintable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basecrdt.core.api.ICRDTOperation;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDT;
import org.apache.bifromq.basecrdt.core.exception.CRDTNotFoundException;
import org.apache.bifromq.basecrdt.core.internal.CausalCRDTInflaterFactory;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.compressor.Compressor;
import org.apache.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import org.apache.bifromq.basecrdt.store.proto.MessagePayload;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class CRDTStore implements ICRDTStore {
    private final Logger log;
    private final String storeId;
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final CRDTStoreOptions options;
    private final CausalCRDTInflaterFactory inflaterFactory;
    private final Subject<CRDTStoreMessage> storeMsgPublisher = PublishSubject.<CRDTStoreMessage>create()
        .toSerialized();
    private final Map<String, AntiEntropyManager> antiEntroyMgrs = Maps.newConcurrentMap();
    private final Compressor compressor;
    private final CompositeDisposable disposable = new CompositeDisposable();
    private final ScheduledExecutorService storeExecutor;
    private final MetricManager metricManager;

    public CRDTStore(CRDTStoreOptions options) {
        this.options = options;
        this.storeId = options.id();
        this.log = MDCLogger.getLogger(CRDTStore.class, "store", storeId);
        storeExecutor = options.storeExecutor();
        String[] tags = new String[] {"store.id", storeId};
        inflaterFactory = new CausalCRDTInflaterFactory(
            options.id(),
            options.inflationInterval(),
            options.orHistoryExpireTime(),
            options.maxCompactionTime(),
            storeExecutor,
            tags);
        compressor = Compressor.newInstance(options.compressAlgorithm());
        metricManager = new MetricManager(Tags.of(tags));
    }

    @Override
    public String id() {
        return storeId;
    }

    @Override
    public <O extends ICRDTOperation, T extends ICausalCRDT<O>> T host(Replica replicaId, ByteString localAddr) {
        checkState();
        AntiEntropyManager antiEntropyMgr = antiEntroyMgrs.computeIfAbsent(replicaId.getUri(),
            k -> new AntiEntropyManager(storeId,
                localAddr,
                inflaterFactory.create(replicaId),
                storeExecutor,
                options.maxEventsInDelta()));
        antiEntropyMgr.neighborMessages()
            .subscribe(t -> storeMsgPublisher.onNext(CRDTStoreMessage.newBuilder()
                .setUri(replicaId.getUri())
                .setSender(localAddr)
                .setReceiver(t.neighborAddress())
                .setPayload(compressToPayload(compressor, t.deltaMsg()))
                .build()));
        return antiEntropyMgr.crdt();
    }

    @Override
    public CompletableFuture<Void> stopHosting(Replica replicaId) {
        checkState();
        AntiEntropyManager antiEntropyMgr = antiEntroyMgrs.remove(replicaId.getUri());
        if (antiEntropyMgr == null) {
            return CompletableFuture.failedFuture(new CRDTNotFoundException());
        }
        return antiEntropyMgr.stop();
    }

    @Override
    public void join(Replica replicaId, Set<ByteString> memberAddrs) {
        checkState();
        AntiEntropyManager antiEntropyMgr = antiEntroyMgrs.get(replicaId.getUri());
        if (antiEntropyMgr == null) {
            throw new CRDTNotFoundException();
        }
        TreeSet<ByteString> sortedMembers = new TreeSet<>(ByteString.unsignedLexicographicalComparator());
        // make sure local address is a cluster member
        sortedMembers.add(antiEntropyMgr.localAddr());
        sortedMembers.addAll(memberAddrs);
        // determine the neighbors of localAddr
        Set<ByteString> neighborAddrs = PartialMesh.neighbors(sortedMembers, antiEntropyMgr.localAddr());
        antiEntropyMgr.setNeighbors(neighborAddrs);
    }

    @Override
    public Observable<CRDTStoreMessage> storeMessages() {
        checkState();
        return storeMsgPublisher;
    }

    @Override
    public void start(Observable<CRDTStoreMessage> replicaMessages) {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            disposable.add(replicaMessages
                .subscribeOn(Schedulers.from(storeExecutor))
                .subscribe(msg -> {
                    if (started()) {
                        handleStoreMessage(msg);
                    }
                }));
            state.set(State.STARTED);
            log.debug("Started CRDTStore[{}]", id());
        } else {
            log.warn("Start more than one time");
        }
    }

    @Override
    public void stop() {
        if (state.compareAndSet(State.STARTED, State.STOPPED)) {
            log.debug("Stop CRDTStore[{}]", id());
            antiEntroyMgrs.forEach((uri, aaMgr) -> aaMgr.stop());
            antiEntroyMgrs.clear();
            metricManager.close();
            disposable.dispose();
            state.set(State.STOPPED);
        }
    }

    private void handleStoreMessage(CRDTStoreMessage msg) {
        AntiEntropyManager antiEntropyMgr = antiEntroyMgrs.get(msg.getUri());
        if (antiEntropyMgr != null && antiEntropyMgr.localAddr().equals(msg.getReceiver())) {
            MessagePayload payload = decompress(compressor, msg);
            switch (payload.getMsgTypeCase()) {
                case DELTA -> antiEntropyMgr.receive(payload.getDelta(), msg.getSender())
                    .thenAccept(ack -> storeMsgPublisher.onNext(CRDTStoreMessage.newBuilder()
                        .setUri(msg.getUri())
                        .setSender(msg.getReceiver())
                        .setReceiver(msg.getSender())
                        .setPayload(compressToPayload(compressor, ack))
                        .build()));
                case ACK -> antiEntropyMgr.receive(payload.getAck(), msg.getSender());
                default -> log.warn("Unknown message type: {}", payload.getMsgTypeCase());
            }
        } else {
            log.debug("No anti-entropy manager of crdt[{}] bind to addr[{}], ignore the message from addr[{}]",
                msg.getUri(), toPrintable(msg.getReceiver()), toPrintable(msg.getSender()));
        }
    }

    private boolean started() {
        return state.get() == State.STARTED;
    }

    private void checkState() {
        Preconditions.checkState(started(), "Not started");
    }

    private enum State {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private class MetricManager {
        final Gauge objectNumGauge;


        MetricManager(Tags tags) {
            objectNumGauge = Gauge.builder("basecrdt.objectnum", CRDTStore.this,
                    r -> r.antiEntroyMgrs.size())
                .tags(tags)
                .register(Metrics.globalRegistry);

        }

        void close() {
            Metrics.globalRegistry.removeByPreFilterId(objectNumGauge.getId());
        }
    }
}
