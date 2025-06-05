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


import static org.apache.bifromq.basecrdt.store.ReplicaIdGenerator.generate;

import org.apache.bifromq.basecrdt.core.api.ICRDTOperation;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDT;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CRDTStoreTestCluster {
    @AllArgsConstructor
    public static class CRDTStoreMeta {
        final CRDTStoreOptions options;
        final double packetLossPercent;
        final long packetDelayTime;
        final boolean packetRandom;
    }

    private final Map<String, CRDTStoreMeta> storeOptionsMap = Maps.newConcurrentMap();
    private final Map<String, ICRDTStore> storeMap = Maps.newConcurrentMap();
    private final Map<String, Subject<CRDTStoreMessage>> storeReceiverMap = Maps.newConcurrentMap();
    private final CompositeDisposable disposables = new CompositeDisposable();

    public List<String> stores() {
        return Lists.newArrayList(storeMap.keySet());
    }

    public String newStore(CRDTStoreMeta meta) {
        storeOptionsMap.computeIfAbsent(meta.options.id(), k -> {
            loadStore(meta);
            return meta;
        });
        return meta.options.id();
    }

    public void stopStore(String storeId) {
        storeMap.remove(storeId).stop();
        storeReceiverMap.remove(storeId);
    }

    public <O extends ICRDTOperation, T extends ICausalCRDT<O>> T host(String storeId, String uri) {
        Replica replicaId = generate(uri, ByteString.copyFromUtf8(storeId));
        T replica = getStore(storeId).host(replicaId, replicaId.getId());
        NavigableSet<ByteString> members = Sets.newTreeSet(ByteString.unsignedLexicographicalComparator());
        members.add(replica.id().getId());
        getStore(storeId).join(replica.id(), members);
        return replica;
    }

    public void join(String storeId, Replica replicaId, Replica... members) {
        Set<ByteString> memberAddrs = Sets.newHashSet();
        for (Replica replica : members) {
            memberAddrs.add(replica.getId());
        }
        getStore(storeId).join(replicaId, memberAddrs);
    }

    public ICRDTStore getStore(String storeId) {
        return storeMap.get(storeId);
    }

    private String loadStore(CRDTStoreMeta meta) {
        Subject<CRDTStoreMessage> receiverSubject = PublishSubject.<CRDTStoreMessage>create().toSerialized();
        Observable<CRDTStoreMessage> receiver = receiverSubject;
        if (meta.packetLossPercent > 0) {
            receiver = receiverSubject
                .filter(t -> ThreadLocalRandom.current().nextDouble() > meta.packetLossPercent);
        }
        if (meta.packetDelayTime > 0) {
            receiver = receiverSubject.flatMap(t -> Observable.just(t) // reorder
                .delay(meta.packetDelayTime, TimeUnit.MILLISECONDS));
        }
        if (meta.packetRandom) {
            receiver = receiverSubject.flatMap(t -> Observable.just(t) // reorder
                .delay(ThreadLocalRandom.current().nextInt(2000), TimeUnit.MILLISECONDS));
        }
        ICRDTStore store = ICRDTStore.newInstance(meta.options);
        store.start(receiver);
        storeMap.put(store.id(), store);
        storeReceiverMap.put(store.id(), receiverSubject);
        disposables.add(store.storeMessages()
            .observeOn(Schedulers.io())
            .subscribe(msg -> {
                String storeId = msg.getReceiver().toStringUtf8();
                if (storeReceiverMap.containsKey(storeId)) {
//                        log.trace("Forward message {} to target store[{}]", msg, storeId);
//                        Log.info(log, "Store[{}] forward {} message[size:{}] to target store[{}]:\n{}",
//                                store.id(), msg.getMsgTypeCase(), msg.getSerializedSize(), storeId,
//                                (Log.Stringify) () -> {
//                                    try {
//                                        switch (msg.getMsgTypeCase()) {
//                                            case DELTA:
//                                                DeltaMessage.Builder builder = msg.getDelta().toBuilder();
//                                                for (int i = 0; i < builder.getReplacementCount(); i++) {
//                                                    Replacement replacement = builder.getReplacement(i);
//                                                    if (replacement.getDots(0).hasLattice()) {
//                                                        builder.setReplacement(i, replacement.toBuilder()
//                                                                .setDots(0, replacement.getDots(0).toBuilder()
//                                                                        .setLattice(StateLattice.getDefaultInstance())
//                                                                        .build())
//                                                                .build());
//                                                    }
//                                                }
//                                                return JsonFormat.printer()
//                                                        .print(msg.toBuilder().setDelta(builder.build()).build());
//                                            case ACK:
//                                            default:
//                                                return JsonFormat.printer().print(msg);
//                                        }
//                                    } catch (Exception e) {
//                                        return msg.toString();
//                                    }
//                                });
                    storeReceiverMap.get(storeId).onNext(msg);
                } else {
                    log.debug("Drop message {} from store[{}]", msg, msg.getSender().toStringUtf8());
                }
            }));
        return store.id();
    }

    public void shutdown() {
        disposables.dispose();
        storeMap.keySet().forEach(this::stopStore);
    }
}
