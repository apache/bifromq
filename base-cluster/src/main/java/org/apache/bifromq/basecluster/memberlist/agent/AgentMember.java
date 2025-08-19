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

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import org.apache.bifromq.basecluster.agent.proto.AgentMessage;
import org.apache.bifromq.basecluster.agent.proto.AgentMessageEnvelope;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basehlc.HLC;

@Slf4j
class AgentMember implements IAgentMember {
    private final AgentMemberAddr localAddr;
    private final IORMap agentCRDT;
    private final IAgentMessenger messenger;
    private final Supplier<Set<AgentMemberAddr>> memberAddresses;
    private final PublishSubject<AgentMessage> agentMessageSubject = PublishSubject.create();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final ReadWriteLock destroyLock = new ReentrantReadWriteLock();
    private final AtomicReference<AgentMemberMetadata> metadata = new AtomicReference<>(
        AgentMemberMetadata.newBuilder().setHlc(HLC.INST.get()).build());
    private volatile boolean destroy = false;

    AgentMember(AgentMemberAddr memberAddr,
                IORMap agentCRDT,
                IAgentMessenger messenger,
                Scheduler scheduler,
                Supplier<Set<AgentMemberAddr>> memberAddresses) {
        this.localAddr = memberAddr;
        this.agentCRDT = agentCRDT;
        this.messenger = messenger;
        this.memberAddresses = memberAddresses;
        updateCRDT();
        disposables.add(agentCRDT.inflation()
            .observeOn(scheduler)
            .subscribe(this::updateCRDT));
        disposables.add(messenger.receive()
            .filter(msg -> msg.getReceiver().equals(localAddr))
            .map(AgentMessageEnvelope::getMessage)
            .observeOn(scheduler)
            .subscribe(agentMessageSubject::onNext));
    }

    @Override
    public AgentMemberMetadata metadata() {
        return metadata.get();
    }

    @Override
    public void metadata(ByteString value) {
        skipRunWhenDestroyed(() -> {
            if (!metadata.get().getValue().equals(value)) {
                metadata.set(AgentMemberMetadata.newBuilder().setValue(value).setHlc(HLC.INST.get()).build());
                updateCRDT();
            }
        });
    }

    @Override
    public AgentMemberAddr address() {
        return localAddr;
    }

    @Override
    public CompletableFuture<Void> broadcast(ByteString message, boolean reliable) {
        return throwsWhenDestroyed(() -> {
            AgentMessage agentMessage = AgentMessage.newBuilder().setSender(localAddr).setPayload(message).build();
            return CompletableFuture.allOf(memberAddresses.get().stream()
                .map(memberAddr -> messenger.send(agentMessage, memberAddr, reliable))
                .toArray(CompletableFuture[]::new)).exceptionally(e -> null);
        });
    }

    @Override
    public CompletableFuture<Void> send(AgentMemberAddr targetMemberAddr, ByteString message, boolean reliable) {
        return throwsWhenDestroyed(() -> {
            if (memberAddresses.get().contains(targetMemberAddr)) {
                AgentMessage agentMessage = AgentMessage.newBuilder().setSender(localAddr).setPayload(message).build();
                return messenger.send(agentMessage, targetMemberAddr, reliable);
            }
            return CompletableFuture.failedFuture(new UnknownHostException("Target not found"));
        });
    }

    @Override
    public CompletableFuture<Void> multicast(String targetMemberName, ByteString message, boolean reliable) {
        return throwsWhenDestroyed(() -> {
            Set<AgentMemberAddr> targetAddrs = memberAddresses.get().stream()
                .filter(memberAddr -> memberAddr.getName().equals(targetMemberName))
                .collect(Collectors.toSet());
            AgentMessage agentMessage = AgentMessage.newBuilder()
                .setSender(localAddr)
                .setPayload(message)
                .build();
            return CompletableFuture.allOf(targetAddrs.stream()
                .map(targetAddr -> messenger.send(agentMessage, targetAddr, reliable))
                .toArray(CompletableFuture[]::new));
        });
    }

    private void updateCRDT(long ts) {
        skipRunWhenDestroyed(() -> {
            Optional<AgentMemberMetadata> metaOnCRDT = CRDTUtil.getAgentMemberMetadata(agentCRDT, localAddr);
            if (metaOnCRDT.isEmpty() || !metaOnCRDT.get().equals(metadata.get())) {
                updateCRDT();
            }
        });
    }

    private void updateCRDT() {
        skipRunWhenDestroyed(() -> agentCRDT.execute(ORMapOperation.update(localAddr.toByteString())
            .with(MVRegOperation.write(metadata.get().toByteString()))));
    }

    @Override
    public Observable<AgentMessage> receive() {
        return agentMessageSubject;
    }

    @Override
    public void refresh() {
        skipRunWhenDestroyed(() -> {
            metadata.set(metadata.get().toBuilder().setHlc(HLC.INST.get()).build());
            updateCRDT();
        });
    }

    private void skipRunWhenDestroyed(Runnable runnable) {
        Lock readLock = destroyLock.readLock();
        try {
            readLock.lock();
            if (destroy) {
                return;
            }
            runnable.run();
        } finally {
            readLock.unlock();
        }
    }

    private CompletableFuture<Void> throwsWhenDestroyed(Supplier<CompletableFuture<Void>> callable) {
        Lock readLock = destroyLock.readLock();
        try {
            readLock.lock();
            if (destroy) {
                throw new IllegalStateException("Agent member has been deregistered");
            }
            return callable.get();
        } finally {
            readLock.unlock();
        }
    }

    CompletableFuture<Void> destroy() {
        Lock writeLock = destroyLock.writeLock();
        try {
            writeLock.lock();
            if (destroy) {
                return CompletableFuture.completedFuture(null);
            }
            return agentCRDT.execute(ORMapOperation.remove(localAddr.toByteString()).of(mvreg))
                .whenComplete((v, e) -> {
                    disposables.dispose();
                    agentMessageSubject.onComplete();
                    destroy = true;
                });
        } finally {
            writeLock.unlock();
        }
    }
}
