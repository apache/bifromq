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

package org.apache.bifromq.basekv.store.range;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The interface of the range finite state machine.
 */
public interface IKVRangeFSM {

    KVRangeId id();

    long ver();

    Boundary boundary();

    Observable<KVRangeDescriptor> describe();

    CompletableFuture<Void> open(IKVRangeMessenger messenger);

    void tick();

    /**
     * Close the range, all activities of the range will stop after closed.
     *
     * @return a future that will be completed when the range is closed
     */
    CompletableFuture<Void> close();

    /**
     * Destroy the range from store, all range related data will be purged from the store. The range will be closed
     * implicitly
     *
     * @return a future that will be completed when the range is destroyed
     */
    CompletableFuture<Void> destroy();

    /**
     * Recover the range quorum, if it's unable to do election.
     *
     * @return a future that will be completed when the range is recovered
     */
    CompletableFuture<Void> recover();

    CompletableFuture<Void> transferLeadership(long ver, String newLeader);

    CompletableFuture<Void> changeReplicaConfig(long ver, Set<String> newVoters, Set<String> newLearners);

    CompletableFuture<Void> split(long ver, ByteString splitKey);

    CompletableFuture<Void> merge(long ver, KVRangeId mergeeId);

    CompletableFuture<Boolean> exist(long ver, ByteString key, boolean linearized);

    CompletableFuture<Optional<ByteString>> get(long ver, ByteString key, boolean linearized);

    CompletableFuture<ROCoProcOutput> queryCoProc(long ver, ROCoProcInput query, boolean linearized);

    CompletableFuture<ByteString> put(long ver, ByteString key, ByteString value);

    CompletableFuture<ByteString> delete(long ver, ByteString key);

    CompletableFuture<RWCoProcOutput> mutateCoProc(long ver, RWCoProcInput mutate);

}
