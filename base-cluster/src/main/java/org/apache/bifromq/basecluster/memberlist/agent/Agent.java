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

package org.apache.bifromq.basecluster.memberlist.agent;

import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.mvreg;
import static java.util.Collections.emptyMap;

import org.apache.bifromq.basecluster.agent.proto.AgentEndpoint;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.ICRDTStore;
import com.google.common.collect.Sets;
import com.google.protobuf.AbstractMessageLite;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Agent implements IAgent {
    private enum State {
        JOINED, QUITTING, QUITED
    }

    private final ReadWriteLock quitLock = new ReentrantReadWriteLock();
    private final String agentId;
    private final AgentEndpoint localEndpoint;
    private final AtomicReference<State> state = new AtomicReference<>(State.JOINED);
    private final IAgentMessenger messenger;
    private final Scheduler scheduler;
    private final ICRDTStore store;
    private final IORMap agentCRDT;
    private final Map<AgentMemberAddr, AgentMember> localMemberRegistry = new ConcurrentHashMap<>();
    private final BehaviorSubject<Map<AgentMemberAddr, AgentMemberMetadata>> agentMembersSubject =
        BehaviorSubject.createDefault(emptyMap());
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final Gauge memberNumGauge;
    private volatile Set<AgentEndpoint> currentAgentEndpoints = new HashSet<>();

    public Agent(String agentId,
                 AgentEndpoint endpoint,
                 IAgentMessenger messenger,
                 Scheduler scheduler,
                 ICRDTStore store,
                 IAgentAddressProvider hostProvider,
                 String... tags) {
        this.agentId = agentId;
        this.localEndpoint = endpoint;
        this.messenger = messenger;
        this.scheduler = scheduler;
        this.store = store;
        // using hostEndpoint as replicaId and localAddress
        agentCRDT = store.host(Replica.newBuilder()
            .setUri(CRDTUtil.toAgentURI(agentId))
            .setId(localEndpoint.toByteString())
            .build(), localEndpoint.toByteString());
        disposables.add(agentCRDT.inflation()
            .observeOn(scheduler)
            .subscribe(this::sync));
        disposables.add(hostProvider.agentAddress()
            .observeOn(scheduler)
            .subscribe(this::handleAgentEndpointsUpdate));
        memberNumGauge = Gauge.builder("basecluster.agent.members", () -> agentMembersSubject.getValue().size())
            .tags(tags)
            .tags("id", agentId)
            .register(Metrics.globalRegistry);
    }

    @Override
    public String id() {
        return agentId;
    }

    @Override
    public AgentEndpoint local() {
        return localEndpoint;
    }

    @Override
    public Observable<Map<AgentMemberAddr, AgentMemberMetadata>> membership() {
        return agentMembersSubject;
    }

    @Override
    public IAgentMember register(String memberName) {
        return runIfJoined(() -> {
            AgentMemberAddr memberAddr = AgentMemberAddr.newBuilder()
                .setName(memberName)
                .setEndpoint(localEndpoint.getEndpoint())
                .setIncarnation(localEndpoint.getIncarnation())
                .build();
            return localMemberRegistry.computeIfAbsent(memberAddr,
                k -> new AgentMember(memberAddr, agentCRDT, messenger, scheduler,
                    () -> agentMembersSubject.getValue().keySet()));
        });
    }

    @Override
    public CompletableFuture<Void> deregister(IAgentMember member) {
        return runIfJoined(() -> {
            if (localMemberRegistry.remove(member.address(), member)) {
                return ((AgentMember) member).destroy();
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    public CompletableFuture<Void> quit() {
        Lock writeLock = quitLock.writeLock();
        try {
            writeLock.lock();
            if (state.compareAndSet(State.JOINED, State.QUITTING)) {
                // stop react to host update and inflation
                return CompletableFuture.allOf(localMemberRegistry.values().stream()
                        .map(AgentMember::destroy)
                        .toArray(CompletableFuture[]::new))
                    .thenCompose(v -> {
                        disposables.dispose();
                        agentMembersSubject.onComplete();
                        return store.stopHosting(agentCRDT.id());
                    })
                    .whenComplete((v, e) -> state.set(State.QUITED));
            } else if (state.get() == State.QUITTING) {
                return CompletableFuture.failedFuture(new IllegalStateException("quit has started"));
            } else {
                return CompletableFuture.completedFuture(null);
            }
        } finally {
            writeLock.unlock();
            Metrics.globalRegistry.remove(memberNumGauge);
        }
    }

    private void sync(long ts) {
        skipRunIfNotJoined(() -> {
            Map<AgentMemberAddr, AgentMemberMetadata> agentMembersCRDT = CRDTUtil.toAgentMemberMap(agentCRDT);
            Map<AgentMemberAddr, AgentMemberMetadata> agentMembersLocal = new HashMap<>();
            localMemberRegistry.values().forEach(member -> agentMembersLocal.put(member.address(), member.metadata()));
            for (AgentMemberAddr memberAddr : Sets.difference(agentMembersCRDT.keySet(), agentMembersLocal.keySet())) {
                if (memberAddr.getEndpoint().equals(localEndpoint.getEndpoint())) {
                    // obsolete member
                    agentCRDT.execute(ORMapOperation.remove(memberAddr.toByteString()).of(mvreg));
                }
            }
            agentMembersCRDT.putAll(agentMembersLocal);
            agentMembersSubject.onNext(agentMembersCRDT);
        });
    }

    private void handleAgentEndpointsUpdate(Set<AgentEndpoint> agentEndpoints) {
        skipRunIfNotJoined(() -> {
            Set<AgentEndpoint> newAgentEndpoints = Sets.newHashSet(agentEndpoints);
            newAgentEndpoints.add(localEndpoint);
            Set<AgentEndpoint> leftHosts = Sets.difference(currentAgentEndpoints, newAgentEndpoints);
            // drop members on left hosts
            Map<AgentMemberAddr, AgentMemberMetadata> agentMemberMap = CRDTUtil.toAgentMemberMap(agentCRDT);
            for (AgentMemberAddr memberAddr : agentMemberMap.keySet()) {
                AgentEndpoint agentEndpoint = AgentEndpoint.newBuilder()
                    .setEndpoint(memberAddr.getEndpoint())
                    .setIncarnation(memberAddr.getIncarnation())
                    .build();
                if (leftHosts.contains(agentEndpoint)) {
                    agentCRDT.execute(ORMapOperation.remove(memberAddr.toByteString()).of(mvreg));
                }
            }
            // update landscape
            currentAgentEndpoints = newAgentEndpoints;
            store.join(agentCRDT.id(),
                currentAgentEndpoints.stream().map(AbstractMessageLite::toByteString).collect(Collectors.toSet()));
        });
    }

    private void skipRunIfNotJoined(Runnable runnable) {
        Lock readLock = quitLock.readLock();
        try {
            readLock.lock();
            if (state.get() != State.JOINED) {
                return;
            }
            runnable.run();
        } finally {
            readLock.unlock();
        }

    }

    private <T> T runIfJoined(Supplier<T> supplier) {
        Lock readLock = quitLock.readLock();
        try {
            readLock.lock();
            if (state.get() != State.JOINED) {
                throw new IllegalArgumentException("Agent has quit");
            }
            return supplier.get();
        } finally {
            readLock.unlock();
        }
    }
}
