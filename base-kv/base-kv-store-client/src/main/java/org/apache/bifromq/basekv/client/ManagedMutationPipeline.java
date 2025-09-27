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

package org.apache.bifromq.basekv.client;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.store.proto.KVRangeRWReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.baserpc.client.IRPCClient;
import org.slf4j.Logger;

class ManagedMutationPipeline implements IMutationPipeline {
    private final Logger log;
    private final Disposable disposable;
    private final Consumer<KVRangeDescriptor> routePatcher;
    private final Executor clientExecutor;
    private volatile IRPCClient.IRequestPipeline<KVRangeRWRequest, KVRangeRWReply> ppln;

    ManagedMutationPipeline(Observable<IRPCClient.IRequestPipeline<KVRangeRWRequest, KVRangeRWReply>> pplnObservable,
                            Consumer<KVRangeDescriptor> routePatcher,
                            Executor clientExecutor,
                            Logger log) {
        this.log = log;
        this.routePatcher = routePatcher;
        this.clientExecutor = clientExecutor;
        disposable = pplnObservable.subscribe(next -> {
            IRPCClient.IRequestPipeline<KVRangeRWRequest, KVRangeRWReply> old = ppln;
            ppln = next;
            if (old != null) {
                old.close();
            }
        });
    }

    @Override

    public CompletableFuture<KVRangeRWReply> execute(KVRangeRWRequest request) {
        log.trace("Requesting rw range:req={}", request);
        return ppln.invoke(request)
            .thenApplyAsync(v -> {
                if (v.hasLatest()) {
                    routePatcher.accept(v.getLatest());
                }
                return v;
            }, clientExecutor);
    }

    @Override
    public void close() {
        disposable.dispose();
        if (ppln != null) {
            ppln.close();
        }
    }
}
