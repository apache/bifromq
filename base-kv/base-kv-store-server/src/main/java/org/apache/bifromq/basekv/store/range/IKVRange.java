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

package org.apache.bifromq.basekv.store.range;

import io.reactivex.rxjava3.core.Observable;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;

/**
 * Interface for accessing and updating a key-value range.
 */
public interface IKVRange extends IKVRangeIdentifiable {

    /**
     * Get the observable of version.
     *
     * @return the observable
     */
    Observable<Long> ver();

    /**
     * Get the current version.
     *
     * @return the current version
     */
    long currentVer();

    /**
     * Get the observable of state.
     *
     * @return the observable
     */
    Observable<State> state();

    /**
     * Get the current state.
     *
     * @return the current state
     */
    State currentState();

    /**
     * Get the observable of cluster config.
     *
     * @return the observable
     */
    Observable<ClusterConfig> clusterConfig();

    /**
     * Get the current cluster config.
     *
     * @return the current cluster config
     */
    ClusterConfig currentClusterConfig();

    /**
     * Get the observable of boundary.
     *
     * @return the observable
     */
    Observable<Boundary> boundary();

    /**
     * Get the current boundary.
     *
     * @return the current boundary
     */
    Boundary currentBoundary();

    /**
     * Get the observable of last applied index.
     *
     * @return the observable
     */
    Observable<Long> lastAppliedIndex();

    /**
     * Get the current last applied index.
     *
     * @return the current last applied index
     */
    long currentLastAppliedIndex();

    /**
     * Make a checkpoint of current state and return a descriptor.
     *
     * @return the descriptor of the checkpoint
     */
    KVRangeSnapshot checkpoint();

    /**
     * Check if the given checkpoint exists.
     *
     * @param checkpoint the descriptor
     * @return bool
     */
    boolean hasCheckpoint(KVRangeSnapshot checkpoint);

    /**
     * Open a reader for accessing the checkpoint data.
     *
     * @param checkpoint the descriptor
     * @return the checkpoint reader
     */
    IKVRangeReader open(KVRangeSnapshot checkpoint);

    /**
     * Create a refreshable consistent-view reader.
     *
     * @return the reader
     */
    IKVRangeRefreshableReader newReader();

    /**
     * Get a writer for updating the range.
     *
     * @return the range writer
     */
    IKVRangeWriter<?> toWriter();

    /**
     * Get a writer for updating the range and using the provided recorder recording write load.
     *
     * @param recorder the load recorder
     * @return the range writer
     */
    IKVRangeWriter<?> toWriter(IKVLoadRecorder recorder);

    /**
     * Open a restore session to receiving data from given snapshot.
     *
     * @param snapshot the snapshot
     * @param progressListener the progress listener
     * @return the session
     */
    IKVRangeRestoreSession startRestore(KVRangeSnapshot snapshot,
                                        IKVRangeRestoreSession.IKVRestoreProgressListener progressListener);

    /**
     * The current estimated size of the KVRange.
     *
     * @return the size in bytes
     */
    long size();

    /**
     * Close the KVRange.
     */
    void close();

    /**
     * Close and destroy the KVRange.
     */
    void destroy();
}
