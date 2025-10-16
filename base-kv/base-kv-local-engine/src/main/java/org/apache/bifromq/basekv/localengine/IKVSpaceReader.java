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

package org.apache.bifromq.basekv.localengine;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.apache.bifromq.basekv.proto.Boundary;

/**
 * The interface of a consistent-view reader for a KV space.
 */
public interface IKVSpaceReader extends IKVSpaceIdentifiable, AutoCloseable {
    /**
     * Get the metadata in key-value pair.
     *
     * @param metaKey the key of the metadata
     * @return the value of the metadata
     */
    Optional<ByteString> metadata(ByteString metaKey);

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
     * Create a new iterator for the space.
     *
     * @return the iterator
     */
    IKVSpaceIterator newIterator();

    /**
     * Create a new iterator for a sub-boundary of the space.
     *
     * @param subBoundary the sub-boundary
     * @return the iterator
     */
    IKVSpaceIterator newIterator(Boundary subBoundary);

    /**
     * Get the estimated size of the data in the specified boundary.
     *
     * @param boundary the boundary
     * @return the size of the space in the specified boundary
     */
    long size(Boundary boundary);

    /**
     * Close the reader and release all resources.
     */
    void close();
}
