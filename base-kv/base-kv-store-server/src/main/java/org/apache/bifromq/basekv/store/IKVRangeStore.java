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

package org.apache.bifromq.basekv.store;

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;

/**
 * The interface of a KVRangeStore, which is responsible for hosting a KVRange.
 */
public interface IKVRangeStore {
    /**
     * The id of base-kv cluster the current store belongs to.
     *
     * @return the cluster id
     */
    String clusterId();

    /**
     * The id of current store.
     *
     * @return the store id
     */
    String id();

    /**
     * If the store is started.
     *
     * @return true if the store is started, false otherwise
     */
    boolean isStarted();

    /**
     * Start the store by passing in a messenger which is used for inter-store communication.
     *
     * @param messenger the messenger
     */
    void start(IStoreMessenger messenger);

    /**
     * Stop the store.
     */
    void stop();

    /**
     * Bootstrap a KVRange on current store.
     *
     * @return future true if success or false if the range is already hosted by current store
     */
    CompletableFuture<Boolean> bootstrap(KVRangeId rangeId, Boundary boundary);

    boolean isHosting(KVRangeId rangeId);

    /**
     * Recover lost-quorum ranges hosted in current store.
     *
     * @return future of the task
     */
    CompletionStage<Void> recover(KVRangeId rangeId);

    /**
     * Quit a zombie KVRange hosted in current store. A 'zombie' state is detected when the KVRange kept in Candidate state.
     *
     * @param rangeId the id of the zombie KVRange
     * @return the future of the quit task
     */
    CompletionStage<Boolean> quit(KVRangeId rangeId);

    /**
     * The observable of the KVRangeStoreDescriptor, which is used for store discovery.
     *
     * @return the observable of the KVRangeStoreDescriptor
     */
    Observable<KVRangeStoreDescriptor> describe();

    /**
     * Transfer the leadership of specified KVRange to other KVRangeStore.
     *
     * @param id the id of the KVRange
     * @return the future of the task
     */
    CompletionStage<Void> transferLeadership(long ver, KVRangeId id, String newLeader);

    /**
     * Change a KVRange's config, this method must be invoked on the leader of specified KVRange, and there is no
     * un-stabilized range settings.
     *
     * @param ver the version of the KVRange
     * @param id the id of the KVRange
     * @param newVoters the new voter replicas of the KVRange
     * @param  newLearners the new learner replicas of the KVRange
     * @return the future of the task
     */
    CompletionStage<Void> changeReplicaConfig(long ver, KVRangeId id, Set<String> newVoters, Set<String> newLearners);

    /**
     * Split a KVRange into two KVRanges, the new KVRange will be hosted by the current store.
     *
     * @param ver the version of the KVRange
     * @param id the id of the KVRange
     * @param splitKey the split key
     * @return the future of the task
     */
    CompletionStage<Void> split(long ver, KVRangeId id, ByteString splitKey);

    /**
     * Merge two KVRanges into one KVRange.
     *
     * @param ver the version of the KVRange
     * @param mergerId the id of the KVRange to be merged
     * @param mergeeId the id of the KVRange to be merged into
     * @param mergeeVoters the voters of the mergee KVRange
     * @return the future of the task
     */
    CompletionStage<Void> merge(long ver, KVRangeId mergerId, KVRangeId mergeeId, Set<String> mergeeVoters);

    /**
     * Check if the specified key exists in the KVRange.
     *
     * @param ver the version of the KVRange
     * @param id the id of the KVRange
     * @param key the key to be checked
     * @param linearized if true, the check will be linearized
     * @return the future of the task
     */
    CompletionStage<Boolean> exist(long ver, KVRangeId id, ByteString key, boolean linearized);

    /**
     * Get the value of the specified key in the KVRange.
     *
     * @param ver the version of the KVRange
     * @param id the id of the KVRange
     * @param key the key to be get
     * @param linearized if true, the get will be linearized
     * @return the future of the task
     */
    CompletionStage<Optional<ByteString>> get(long ver, KVRangeId id, ByteString key, boolean linearized);

    /**
     * Execute a read-only co-process on the KVRange.
     *
     * @param ver the version of the KVRange
     * @param id the id of the KVRange
     * @param query the query to be executed
     * @param linearized if true, the query will be linearized
     * @return the future of the task
     */
    CompletionStage<ROCoProcOutput> queryCoProc(long ver, KVRangeId id, ROCoProcInput query, boolean linearized);

    /**
     * Put a key-value pair into the KVRange.
     *
     * @param ver the version of the KVRange
     * @param id the id of the KVRange
     * @param key the key to be put
     * @param value the value to be put
     * @return the future of the task
     */
    CompletionStage<ByteString> put(long ver, KVRangeId id, ByteString key, ByteString value);

    /**
     * Delete a key-value pair from the KVRange.
     *
     * @param ver the version of the KVRange
     * @param id the id of the KVRange
     * @param key the key to be deleted
     * @return the future of the task
     */
    CompletionStage<ByteString> delete(long ver, KVRangeId id, ByteString key);

    /**
     * Execute a read-write co-process on the KVRange.
     *
     * @param ver the version of the KVRange
     * @param id the id of the KVRange
     * @param mutate the mutate to be executed
     * @return the future of the task
     */
    CompletionStage<RWCoProcOutput> mutateCoProc(long ver, KVRangeId id, RWCoProcInput mutate);
}
