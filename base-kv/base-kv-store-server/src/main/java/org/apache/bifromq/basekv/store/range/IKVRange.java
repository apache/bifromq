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
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import org.apache.bifromq.basekv.store.api.IKVReader;
import io.reactivex.rxjava3.core.Observable;

public interface IKVRange extends IKVRangeReader {
    /**
     * Get the observable of metadata.
     *
     * @return the observable
     */
    Observable<KVRangeMeta> metadata();

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
     * Open an iterator for accessing the checkpoint data.
     *
     * @param checkpoint the descriptor
     * @return the checkpoint reader
     */
    IKVRangeCheckpointReader open(KVRangeSnapshot checkpoint);

    IKVReader borrowDataReader();

    void returnDataReader(IKVReader borrowed);

    @Override
    IKVCloseableReader newDataReader();

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

    IKVReseter toReseter(KVRangeSnapshot snapshot);

    void close();

    void destroy();

    /**
     * Metadata about the KVRange.
     *
     * @param ver      the version
     * @param state    the state
     * @param boundary the boundary
     */
    record KVRangeMeta(long ver, State state, Boundary boundary, ClusterConfig clusterConfig) {
    }
}
