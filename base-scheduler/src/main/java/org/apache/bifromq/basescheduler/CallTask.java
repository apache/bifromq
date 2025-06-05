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

package org.apache.bifromq.basescheduler;

import java.util.concurrent.CompletableFuture;

class CallTask<CallT, CallResultT, BatcherKeyT> implements ICallTask<CallT, CallResultT, BatcherKeyT> {
    private final BatcherKeyT batcherKey;
    private final CallT call;
    private final CompletableFuture<CallResultT> resultPromise = new CompletableFuture<>();
    private final long ts = System.nanoTime();

    CallTask(BatcherKeyT batcherKey, CallT call) {
        this.batcherKey = batcherKey;
        this.call = call;
    }

    @Override
    public CallT call() {
        return call;
    }

    @Override
    public CompletableFuture<CallResultT> resultPromise() {
        return resultPromise;
    }

    @Override
    public BatcherKeyT batcherKey() {
        return batcherKey;
    }

    @Override
    public long ts() {
        return ts;
    }
}
