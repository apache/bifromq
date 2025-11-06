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

import static org.apache.bifromq.basecrdt.store.ReplicaIdGenerator.generate;
import static org.apache.bifromq.basecrdt.util.Formatter.print;
import static org.apache.bifromq.basecrdt.util.Formatter.toPrintable;

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.bifromq.basecluster.IAgentHost;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.memberlist.agent.IAgent;
import org.apache.bifromq.basecluster.memberlist.agent.IAgentMember;
import org.apache.bifromq.basecrdt.core.api.ICRDTOperation;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDT;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.ICRDTStore;
import org.apache.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import org.apache.bifromq.baseenv.ZeroCopyParser;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class CRDTCluster<O extends ICRDTOperation, C extends ICausalCRDT<O>> {
    private final Logger log;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final IAgentHost agentHost;
    private final AgentMemberAddr endpoint;
    private final ReadWriteLock shutdownLock = new ReentrantReadWriteLock();
    private final Replica replicaId;
    private final C crdt;
    private final ICRDTStore store;
    private final IAgent membershipAgent;
    private final IAgentMember localMembership;
    private final Subject<CRDTStoreMessage> storeMsgSubject;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final CompletableFuture<Void> quitSignal = new CompletableFuture<>();

    CRDTCluster(String uri,
                ICRDTStore store,
                IAgentHost agentHost,
                Scheduler scheduler,
                Subject<CRDTStoreMessage> storeMsgSubject) {
        this.store = store;
        this.agentHost = agentHost;
        replicaId = generate(uri);
        log = MDCLogger.getLogger(CRDTCluster.class, "store", store.id(), "replica", print(replicaId));
        membershipAgent = agentHost.host(replicaId.getUri());
        endpoint = AgentMemberAddr.newBuilder()
            .setName(AgentUtil.toAgentMemberName(replicaId))
            .setEndpoint(membershipAgent.local().getEndpoint())
            .setIncarnation(membershipAgent.local().getIncarnation())
            .build();
        this.localMembership = membershipAgent.register(endpoint.getName());
        this.storeMsgSubject = storeMsgSubject;
        crdt = store.host(replicaId, endpoint.toByteString());
        disposables.add(membershipAgent.membership()
            .observeOn(scheduler)
            .subscribe(withLock(shutdownLock.readLock(), agentMembers -> {
                if (stopped.get()) {
                    return;
                }
                Set<ByteString> peers = agentMembers.keySet().stream()
                    .map(AbstractMessageLite::toByteString)
                    .collect(Collectors.toSet());
                store.join(replicaId, peers);
            })));
        disposables.add(localMembership.receive()
            .observeOn(scheduler)
            .subscribe(withLock(shutdownLock.readLock(), agentMessage -> {
                if (stopped.get()) {
                    return;
                }
                try {
                    // Parse with aliasing directly from ByteString
                    this.storeMsgSubject.onNext(
                        ZeroCopyParser.parse(agentMessage.getPayload(), CRDTStoreMessage.parser()));
                } catch (InvalidProtocolBufferException e) {
                    log.error("Unable to parse crdt store message from agent message", e);
                }
            })));
        disposables.add(store.storeMessages()
            .filter(msg -> msg.getSender().equals(endpoint.toByteString()))
            .observeOn(scheduler)
            .subscribe(withLock(shutdownLock.readLock(), msg -> {
                if (stopped.get()) {
                    return;
                }
                // Parse receiver with aliasing directly from ByteString
                AgentMemberAddr target = ZeroCopyParser.parse(msg.getReceiver(), AgentMemberAddr.parser()); ;
                localMembership.send(target, msg.toByteString(), true)
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            log.debug("Failed to send store message, uri={}, sender={}, receiver={}",
                                msg.getUri(), target.getName(), endpoint.getName(), e);
                        } else {
                            if (log.isTraceEnabled()) {
                                log.trace("Sent store message, uri={}, sender={}, receiver={}, msg={}",
                                    msg.getUri(), target.getName(), endpoint.getName(), toPrintable(msg));
                            }
                        }
                    });
            })));
    }

    C crdt() {
        return crdt;
    }

    Observable<Set<Replica>> aliveReplicas() {
        return membershipAgent.membership()
            .map(agentMembers -> agentMembers.keySet().stream()
                .map(agentMemberAddr -> AgentUtil.toReplica(agentMemberAddr.getName()))
                .collect(Collectors.toSet()));
    }

    CompletableFuture<Void> close() {
        Lock lock = shutdownLock.writeLock();
        try {
            lock.lock();
            if (stopped.compareAndSet(false, true)) {
                disposables.dispose();
                membershipAgent.deregister(localMembership)
                    .thenCompose(v -> store.stopHosting(replicaId))
                    .thenCompose(v -> agentHost.stopHosting(replicaId.getUri()))
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            log.warn("Error during close", e);
                        }
                        quitSignal.complete(null);
                    });
            }
            return quitSignal;
        } finally {
            lock.unlock();
        }
    }

    private <T> Consumer<T> withLock(Lock lock, Consumer<T> consumer) {
        return (T value) -> {
            try {
                lock.lock();
                consumer.accept(value);
            } finally {
                lock.unlock();
            }
        };
    }
}
