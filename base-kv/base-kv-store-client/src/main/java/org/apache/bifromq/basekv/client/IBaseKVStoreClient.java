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

package org.apache.bifromq.basekv.client;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.store.proto.BootstrapReply;
import org.apache.bifromq.basekv.store.proto.BootstrapRequest;
import org.apache.bifromq.basekv.store.proto.ChangeReplicaConfigReply;
import org.apache.bifromq.basekv.store.proto.ChangeReplicaConfigRequest;
import org.apache.bifromq.basekv.store.proto.KVRangeMergeReply;
import org.apache.bifromq.basekv.store.proto.KVRangeMergeRequest;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.KVRangeRWReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.store.proto.KVRangeSplitReply;
import org.apache.bifromq.basekv.store.proto.KVRangeSplitRequest;
import org.apache.bifromq.basekv.store.proto.RecoverReply;
import org.apache.bifromq.basekv.store.proto.RecoverRequest;
import org.apache.bifromq.basekv.store.proto.TransferLeadershipReply;
import org.apache.bifromq.basekv.store.proto.TransferLeadershipRequest;
import org.apache.bifromq.baserpc.client.IConnectable;
import io.reactivex.rxjava3.core.Observable;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The interface of BaseKV Store Client.
 */
public interface IBaseKVStoreClient extends IConnectable, AutoCloseable {
    static BaseKVStoreClientBuilder newBuilder() {
        return new BaseKVStoreClientBuilder();
    }

    String clusterId();

    Observable<Set<KVRangeStoreDescriptor>> describe();

    NavigableMap<Boundary, KVRangeSetting> latestEffectiveRouter();

    CompletableFuture<BootstrapReply> bootstrap(String storeId, BootstrapRequest request);

    CompletableFuture<RecoverReply> recover(String storeId, RecoverRequest request);

    CompletableFuture<TransferLeadershipReply> transferLeadership(String storeId, TransferLeadershipRequest request);

    CompletableFuture<ChangeReplicaConfigReply> changeReplicaConfig(String storeId, ChangeReplicaConfigRequest request);

    CompletableFuture<KVRangeSplitReply> splitRange(String storeId, KVRangeSplitRequest request);

    CompletableFuture<KVRangeMergeReply> mergeRanges(String storeId, KVRangeMergeRequest request);

    /**
     * Execute a read-write request, the requests from same calling thread will be processed orderly.
     *
     * @param storeId the store id
     * @param request the request
     * @return the future of the reply
     */
    CompletableFuture<KVRangeRWReply> execute(String storeId, KVRangeRWRequest request);

    /**
     * Execute a read-write request, the requests with same orderKey will be processed orderly.
     *
     * @param storeId the store id
     * @param request the request
     * @return the future of the reply
     */
    CompletableFuture<KVRangeRWReply> execute(String storeId, KVRangeRWRequest request, String orderKey);

    /**
     * Execute a read-only query, the requests from same calling thread will be processed orderly.
     *
     * @param storeId the store id
     * @param request the request
     * @return the future of the reply
     */
    CompletableFuture<KVRangeROReply> query(String storeId, KVRangeRORequest request);

    /**
     * Execute a read-only request, the requests with same orderKey will be processed orderly.
     *
     * @param storeId the store id
     * @param request the request
     * @return the future of the reply
     */
    CompletableFuture<KVRangeROReply> query(String storeId, KVRangeRORequest request, String orderKey);

    /**
     * Execute a read-only linearized query, the requests from same calling thread will be processed orderly.
     *
     * @param storeId the store id
     * @param request the request
     * @return the future of the reply
     */
    CompletableFuture<KVRangeROReply> linearizedQuery(String storeId, KVRangeRORequest request);

    /**
     * Execute a read-only linearized request, the requests with same orderKey will be processed orderly.
     *
     * @param storeId the store id
     * @param request the request
     * @return the future of the reply
     */
    CompletableFuture<KVRangeROReply> linearizedQuery(String storeId, KVRangeRORequest request, String orderKey);


    /**
     * Create a caller-managed pipeline for executing rw command orderly.
     *
     * @param storeId the store id
     * @return the mutation pipeline
     */
    IMutationPipeline createMutationPipeline(String storeId);

    /**
     * Create a caller-managed pipeline for execute ro command orderly.
     *
     * @param storeId the store id
     * @return the query pipeline
     */
    IQueryPipeline createQueryPipeline(String storeId);

    /**
     * Create a caller-managed pipeline for execute linearized ro command orderly.
     *
     * @param storeId the store id
     * @return the query pipeline
     */
    IQueryPipeline createLinearizedQueryPipeline(String storeId);

    void close();
}
