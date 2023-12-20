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

package com.baidu.bifromq.basekv.store.api;

import com.baidu.bifromq.basekv.proto.Boundary;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.ROCoProcOutput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcOutput;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface IKVRangeCoProc {
    /**
     * Execute a query co-proc
     *
     * @param input the query input
     * @return the future of query result
     */
    CompletableFuture<ROCoProcOutput> query(ROCoProcInput input, IKVReader client);

    /**
     * Execute a mutation co-proc, returns a supplier of mutation output. The supplier will be called after mutation is
     * persisted successfully.
     *
     * @param input  the mutation input
     * @param reader the range data reader
     * @param writer the range data writer
     * @return the future of mutation result
     */
    Supplier<RWCoProcOutput> mutate(RWCoProcInput input, IKVReader reader, IKVWriter writer);

    /**
     * This method will be called whenever the boundary of the owner KVRange has changed
     *
     * @param boundary the boundary of the owner KVRange
     */
    default void reset(Boundary boundary) {
    }

    /**
     * Close the coproc instance, and release all related resources
     */
    void close();
}
