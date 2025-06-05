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

package org.apache.bifromq.basecrdt.store;

import org.apache.bifromq.basecrdt.core.api.ICRDTOperation;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDT;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.proto.CRDTStoreMessage;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

/**
 * The CRDT store is a distributed store that hosts CRDT replicas and provides the ability to synchronize the replicas.
 */
public interface ICRDTStore {

    /**
     * Construct a new instance.
     *
     * @param options the options
     * @return the new instance
     */
    static ICRDTStore newInstance(@NonNull CRDTStoreOptions options) {
        return new CRDTStore(options);
    }

    /**
     * The global unique id of the store.
     *
     * @return the id
     */
    String id();

    /**
     * Host a CRDT replicaId using the specified replicaId. It's caller's duty to ensure the uniqueness of the provided
     * id within the external managed cluster.
     *
     * @param replicaId the replicaId to host
     * @param localAddr the logical address for the local replicaId
     * @return the hosted CRDT replicaId
     */
    <O extends ICRDTOperation, T extends ICausalCRDT<O>> T host(Replica replicaId, ByteString localAddr);

    /**
     * Stop hosting the replicaId for given uri.
     *
     * @param replicaId the replicaId to stop hosting
     */
    CompletableFuture<Void> stopHosting(Replica replicaId);

    /**
     * Join a memberAddrs from specified local address. Some remote memberAddrs will be selected to be the neighbors
     * with which the local replica keeps synchronizing.
     *
     * @param memberAddrs the list of member address
     */
    void join(Replica replicaId, Set<ByteString> memberAddrs);

    /**
     * An observable of messages generated from current store.
     *
     * @return the observable of messages to be sent
     */
    Observable<CRDTStoreMessage> storeMessages();

    /**
     * Start the store by providing observable of incoming store messages.
     * <br>
     * NOTE: the messages with toStoreId set to '0' is used for broadcast, and will be accepted by every CRDTStore
     *
     * @param replicaMessages the observable to receive messages from others
     */
    void start(Observable<CRDTStoreMessage> replicaMessages);

    /**
     * Stop the store.
     */
    void stop();
}
