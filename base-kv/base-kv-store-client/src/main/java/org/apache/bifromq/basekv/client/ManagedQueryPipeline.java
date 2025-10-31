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
import java.util.function.Consumer;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.baserpc.client.IRPCClient;
import org.slf4j.Logger;

class ManagedQueryPipeline implements IQueryPipeline {
    private final Logger log;
    private final Disposable disposable;
    private final Consumer<KVRangeDescriptor> routePatcher;
    private volatile IRPCClient.IRequestPipeline<KVRangeRORequest, KVRangeROReply> ppln;

    ManagedQueryPipeline(Observable<IRPCClient.IRequestPipeline<KVRangeRORequest, KVRangeROReply>> pplnObservable,
                         Consumer<KVRangeDescriptor> routePatcher,
                         Logger log) {
        this.log = log;
        this.routePatcher = routePatcher;
        disposable = pplnObservable.subscribe(next -> {
            IRPCClient.IRequestPipeline<KVRangeRORequest, KVRangeROReply> old = ppln;
            ppln = next;
            if (old != null) {
                old.close();
            }
        });
    }

    @Override
    public CompletableFuture<KVRangeROReply> query(KVRangeRORequest request) {
        log.trace("Invoke ro range request: \n{}", request);
        return ppln.invoke(request)
            .thenApply(v -> {
                if (v.hasLatest()) {
                    routePatcher.accept(v.getLatest());
                }
                return v;
            });
    }

    @Override
    public void close() {
        disposable.dispose();
        if (ppln != null) {
            ppln.close();
        }
    }
}
