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

package com.baidu.bifromq.basekv.client;

import com.baidu.bifromq.basekv.store.proto.KVRangeROReply;
import com.baidu.bifromq.basekv.store.proto.KVRangeRORequest;
import com.baidu.bifromq.baserpc.IRPCClient;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ManagedQueryPipeline implements IQueryPipeline {
    private final Disposable disposable;
    private volatile IRPCClient.IRequestPipeline<KVRangeRORequest, KVRangeROReply> ppln;

    ManagedQueryPipeline(Observable<IRPCClient.IRequestPipeline<KVRangeRORequest, KVRangeROReply>> pplnObservable) {
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
        return ppln.invoke(request);
    }

    @Override
    public void close() {
        disposable.dispose();
        if (ppln != null) {
            ppln.close();
        }
    }
}
