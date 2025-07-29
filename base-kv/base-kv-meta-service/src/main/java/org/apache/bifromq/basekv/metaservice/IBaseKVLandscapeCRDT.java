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

package org.apache.bifromq.basekv.metaservice;

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.StoreKey;

/**
 * The interface of a BaseKV landscape CRDT.
 */
public interface IBaseKVLandscapeCRDT {
    /**
     * Get the observable of alive replicas of landscape CRDT.
     *
     * @return the observable of alive replicas
     */
    Observable<Set<ByteString>> aliveReplicas();

    /**
     * Get the observable of landscape.
     *
     * @return the observable of landscape
     */
    Observable<Map<StoreKey, KVRangeStoreDescriptor>> landscape();

    /**
     * Get the observed store descriptor of given store.
     *
     * @param storeId the id of the store
     * @return the observed store descriptor
     */
    Optional<KVRangeStoreDescriptor> getStoreDescriptor(String storeId);

    /**
     * Set the store descriptor of given store to CRDT.
     *
     * @param descriptor the store descriptor
     * @return the future of setting store descriptor
     */
    CompletableFuture<Void> setStoreDescriptor(KVRangeStoreDescriptor descriptor);

    /**
     * Remove the store descriptor of given store from CRDT.
     *
     * @param key the key of the store descriptor
     * @return the future of removing store descriptor
     */
    CompletableFuture<Void> removeDescriptor(StoreKey key);

    /**
     * Remove the store descriptor of given store from CRDT.
     *
     * @param storeId the id of the store
     * @return the future of removing store descriptor
     */
    CompletableFuture<Void> removeDescriptor(String storeId);

    /**
     * The key of the store descriptor in CRDT.
     *
     * @param storeId the id of the store
     * @return the descriptor key
     */
    StoreKey toDescriptorKey(String storeId);

    /**
     * Stop the CRDT and release resources.
     */
    void stop();
}
