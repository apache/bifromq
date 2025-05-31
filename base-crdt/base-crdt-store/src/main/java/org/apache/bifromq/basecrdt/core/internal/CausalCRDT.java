/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basecrdt.core.internal;

import org.apache.bifromq.basecrdt.core.api.ICRDTOperation;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDT;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.proto.StateLattice;
import com.google.common.collect.Sets;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class CausalCRDT<T extends IDotStore, O extends ICRDTOperation> implements ICausalCRDT<O> {
    interface DotStoreAccessor<T extends IDotStore> {
        T fetch();
    }

    interface CRDTOperationExecutor<O extends ICRDTOperation> {
        CompletableFuture<Void> submit(O op);
    }

    private final Set<ObservableEmitter<Long>> emitters = Sets.newConcurrentHashSet();

    protected final Replica replica;
    protected final CRDTOperationExecutor<O> executor;
    protected final DotStoreAccessor<T> dotStoreAccessor;

    CausalCRDT(Replica replica, DotStoreAccessor<T> dotStoreAccessor, CRDTOperationExecutor<O> executor) {
        this.replica = replica;
        this.dotStoreAccessor = dotStoreAccessor;
        this.executor = executor;
    }

    @Override
    public Replica id() {
        return replica;
    }

    @Override
    public final CompletableFuture<Void> execute(O op) {
        return executor.submit(op);
    }

    @Override
    public final Observable<Long> inflation() {
        return Observable.create(emitter -> {
            emitters.add(emitter);
            emitter.setCancellable(() -> emitters.remove(emitter));
        });
    }

    /**
     * A notification from inflater about the batch changes made to the dot store.
     *
     * @param addEvents    add events happened
     * @param removeEvents remove events happened
     */
    final void afterInflation(Iterable<StateLattice> addEvents, Iterable<StateLattice> removeEvents) {
        handleInflation(addEvents, removeEvents);
        long ts = System.nanoTime();
        emitters.forEach(e -> e.onNext(ts));
    }

    protected void handleInflation(Iterable<StateLattice> addEvents, Iterable<StateLattice> removeEvents) {
    }

    @Override
    public String toString() {
        return dotStoreAccessor.fetch().toString();
    }
}
