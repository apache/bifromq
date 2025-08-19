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

package org.apache.bifromq.basecrdt.service;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.ICRDTOperation;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDT;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.ICRDTStore;
import org.apache.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import org.apache.bifromq.baseenv.EnvProvider;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

public class CRDTService implements ICRDTService {
    private final Logger log;
    private final ICRDTStore store;
    private final IAgentHost agentHost;
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final Map<String, CRDTCluster<?, ?>> hostedCRDT = Maps.newConcurrentMap(); // key is the uri of crdt
    private final Subject<CRDTStoreMessage> incomingStoreMessages;
    private final ExecutorService executor =
        newSingleThreadExecutor(EnvProvider.INSTANCE.newThreadFactory("crdt-service-scheduler"));
    private final Scheduler scheduler = Schedulers.from(executor);

    public CRDTService(IAgentHost agentHost, CRDTServiceOptions options) {
        this.log = MDCLogger.getLogger(CRDTService.class, "store", options.storeOptions.id());
        this.agentHost = agentHost;
        store = ICRDTStore.newInstance(options.storeOptions);
        incomingStoreMessages = PublishSubject.<CRDTStoreMessage>create().toSerialized();
        start();
    }

    @Override
    public String id() {
        return store.id();
    }

    @Override
    public ByteString agentHostId() {
        checkState();
        return agentHost.local().getId();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <O extends ICRDTOperation, C extends ICausalCRDT<O>> C host(String uri) {
        checkState();
        CRDTCluster<?, ?> crdtContext = hostedCRDT.computeIfAbsent(uri,
            k -> new CRDTCluster<>(k, store, agentHost, scheduler, incomingStoreMessages));
        return (C) crdtContext.crdt();
    }

    @Override
    public CompletableFuture<Void> stopHosting(String uri) {
        checkState();
        assert hostedCRDT.containsKey(uri);
        return stopHostingInternal(uri);
    }

    @Override
    public Observable<Set<Replica>> aliveReplicas(String uri) {
        checkState();
        assert hostedCRDT.containsKey(uri);
        return hostedCRDT.get(uri).aliveReplicas();
    }

    @Override
    public Observable<Set<String>> aliveCRDTs() {
        return agentHost.landscape().map(agentLandscape -> {
            Set<String> crdtUris = new HashSet<>();
            for (Map.Entry<HostEndpoint, Set<String>> entry : agentLandscape.entrySet()) {
                for (String agentId : entry.getValue()) {
                    if (CRDTURI.isValidURI(agentId)) {
                        crdtUris.add(agentId);
                    }
                }
            }
            return crdtUris;
        });
    }

    @Override
    public Observable<Long> refreshSignal() {
        return agentHost.refuteSignal();
    }

    private CompletableFuture<Void> stopHostingInternal(String uri) {
        return hostedCRDT.remove(uri).close();
    }

    private void start() {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            store.start(incomingStoreMessages);
            state.set(State.STARTED);
            log.debug("CRDT service[{}] started", store.id());
        }
    }

    @Override
    public void close() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            log.debug("Stopping CRDT service[{}]", id());
            log.debug("Stop hosting CRDTs");
            CompletableFuture.allOf(hostedCRDT.values()
                    .stream()
                    .map(CRDTCluster::close)
                    .toArray(CompletableFuture[]::new))
                .join();
            log.debug("Stopping CRDT store");
            store.stop();
            log.debug("CRDT service[{}] stopped", id());
            executor.shutdown();
            state.set(State.SHUTDOWN);
        }
    }

    private void checkState() {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
    }

    private enum State {
        INIT, STARTING, STARTED, STOPPING, SHUTDOWN
    }
}
