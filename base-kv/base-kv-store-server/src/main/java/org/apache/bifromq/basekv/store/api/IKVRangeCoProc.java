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

package org.apache.bifromq.basekv.store.api;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * The interface of range co-processor.
 */
public interface IKVRangeCoProc {
    /**
     * Execute a query co-proc.
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
    Supplier<MutationResult> mutate(RWCoProcInput input, IKVReader reader, IKVWriter writer);

    /**
     * This method will be called whenever owner range is restored from a snapshot or boundary changed via split/merge.
     * The returned fact will be included in the KVRangeDescriptor.
     *
     * @param boundary the boundary of the owner KVRange
     * @return the current fact about the data within the range replica.
     */
    default Any reset(Boundary boundary) {
        return Any.getDefaultInstance();
    }

    /**
     * Close the co-proc instance, and release all related resources.
     */
    void close();

    /**
     * The result of mutation co-proc. The output will be returned as the reply to the client, and the fact(if present)
     * will be included in the next KVRangeDescriptor.
     */
    record MutationResult(RWCoProcOutput output, Optional<Any> fact) {
    }
}
