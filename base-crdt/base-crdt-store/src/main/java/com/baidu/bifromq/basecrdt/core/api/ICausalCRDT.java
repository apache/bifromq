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

package com.baidu.bifromq.basecrdt.core.api;

import com.baidu.bifromq.basecrdt.proto.Replica;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;

public interface ICausalCRDT<O extends ICRDTOperation> {
    /**
     * The identity of the CRDT replica
     *
     * @return the id
     */
    Replica id();

    /**
     * The type of the replica
     *
     * @return the CRDT type
     */
    CausalCRDTType type();

    /**
     * Execute an CRDT operation asynchronously
     *
     * @param op the operation
     * @return a future of the execution
     */
    CompletableFuture<Void> execute(O op);

    /**
     * The observable of inflation happens to the CRDT state overtime
     *
     * @return an observable of inflation event
     */
    Observable<Long> inflation();
}
