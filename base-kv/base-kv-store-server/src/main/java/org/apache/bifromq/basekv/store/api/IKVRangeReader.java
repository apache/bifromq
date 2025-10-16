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

package org.apache.bifromq.basekv.store.api;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;

/**
 * Interface for reading a KVRange's consistent-view.
 */
public interface IKVRangeReader extends AutoCloseable {

    /**
     * Get the current version of the KVRange.
     *
     * @return the version of the KVRange
     */
    long version();

    /**
     * Get the current state of the KVRange.
     *
     * @return the state of the KVRange
     */
    State state();

    /**
     * Get the last applied WAL index of the KVRange.
     *
     * @return the last applied WAL index of the KVRange
     */
    long lastAppliedIndex();

    /**
     * Get the current boundary of the KVRange.
     *
     * @return the boundary of the KVRange
     */
    Boundary boundary();

    /**
     * Get the current cluster configuration of the KVRange.
     *
     * @return the cluster configuration of the KVRange
     */
    ClusterConfig clusterConfig();

    /**
     * Get the size of the KVRange within the given boundary intersecting with the KVRange's current boundary.
     *
     * @param boundary the boundary to calculate the size.
     * @return the size of the KVRange within the given boundary
     */
    long size(Boundary boundary);

    /**
     * Check if a key exists.
     *
     * @param key the key
     * @return true if the key exists, false otherwise
     */
    boolean exist(ByteString key);

    /**
     * Get the value of a key.
     *
     * @param key the key
     * @return the value of the key, or empty if the key does not exist
     */
    Optional<ByteString> get(ByteString key);

    /**
     * Get an iterator for the KVRange sharing same consistent-view.
     *
     * @return the iterator.
     */
    IKVIterator iterator();

    /**
     * Get an iterator for a sub-boundary of the KVRange sharing same consistent-view.
     * The sub-boundary is calculated as the intersection of the given boundary and the KVRange's current boundary.
     *
     * @param boundary the boundary
     * @return the iterator.
     */
    IKVIterator iterator(Boundary boundary);

    /**
     * Close the reader and release all resources.
     */
    void close();
}
