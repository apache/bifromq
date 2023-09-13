/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basekv.store.range;

import com.baidu.bifromq.basekv.proto.KVRangeDescriptor;
import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface IKVRange {

    KVRangeId id();

    boolean isOccupying(String checkpointId);

    /**
     * If the range is ready to quit from the store. This happens after config changing or merging operation
     *
     * @return
     */
    boolean readyToQuit();

    Observable<KVRangeDescriptor> describe();

    void open(IKVRangeMessenger messenger);

    void tick();

    /**
     * Close the range, all activity of the range will be stopped after closed
     *
     * @return
     */
    CompletionStage<Void> close();

    /**
     * Quit the range from the store, if the range is ready to quit. The range will be closed implicitly.
     *
     * @return
     */
    CompletionStage<Void> quit();

    /**
     * Destroy the range from store, all range related data will be purged from the store. The range will be closed
     * implicitly
     *
     * @return
     */
    CompletionStage<Void> destroy();

    /**
     * Recover the range quorum, if it's unable to do election
     *
     * @return
     */
    CompletableFuture<Void> recover();

    CompletableFuture<Void> transferLeadership(long ver, String newLeader);

    CompletableFuture<Void> changeReplicaConfig(long ver, Set<String> newVoters, Set<String> newLearners);

    CompletableFuture<Void> split(long ver, ByteString splitKey);

    CompletableFuture<Void> merge(long ver, KVRangeId mergeeId);

    CompletableFuture<Boolean> exist(long ver, ByteString key, boolean linearized);

    CompletableFuture<Optional<ByteString>> get(long ver, ByteString key, boolean linearized);

    CompletableFuture<ByteString> queryCoProc(long ver, ByteString query, boolean linearized);

    CompletableFuture<ByteString> put(long ver, ByteString key, ByteString value);

    CompletableFuture<ByteString> delete(long ver, ByteString key);

    CompletableFuture<ByteString> mutateCoProc(long ver, ByteString mutate);

}
