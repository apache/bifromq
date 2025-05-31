/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basecrdt.core.api;

import org.apache.bifromq.basecrdt.proto.Replacement;
import org.apache.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Inflater interface for a causal CRDT.
 *
 * @param <O> operation type
 * @param <C> CRDT type
 */
public interface ICausalCRDTInflater<O extends ICRDTOperation, C extends ICausalCRDT<O>> {
    CausalCRDTType type();

    C getCRDT();

    Replica id();

    CompletableFuture<Void> stop();

    CompletableFuture<Void> join(Iterable<Replacement> delta);

    CompletableFuture<Optional<Iterable<Replacement>>> delta(
        Map<ByteString, NavigableMap<Long, Long>> coveredLatticeEvents,
        Map<ByteString, NavigableMap<Long, Long>> coveredHistoryEvents,
        int maxEvents);

    Map<ByteString, NavigableMap<Long, Long>> latticeEvents();

    Map<ByteString, NavigableMap<Long, Long>> historyEvents();
}
