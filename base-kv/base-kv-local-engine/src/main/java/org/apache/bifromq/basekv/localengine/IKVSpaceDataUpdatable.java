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

package org.apache.bifromq.basekv.localengine;

import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;

/**
 * The interface for update kv space.
 *
 * @param <T> the type of the updater
 */
public interface IKVSpaceDataUpdatable<T extends IKVSpaceDataUpdatable<T>> extends IKVSpaceReader {
    /**
     * Insert a key-value pair in to the range.
     *
     * @param key   the key to insert
     * @param value the value
     */
    T insert(ByteString key, ByteString value);

    /**
     * Put a key-value pair in to the range.
     *
     * @param key   the key to put
     * @param value the value
     */
    T put(ByteString key, ByteString value);

    /**
     * Delete a key from the range.
     *
     * @param key the key to delete
     */
    T delete(ByteString key);

    /**
     * Clear all data in the range.
     */
    T clear();

    /**
     * Clear a boundary of key-value pairs in the range.
     *
     * @param boundary the boundary
     */
    T clear(Boundary boundary);

    /**
     * Migrate data in given boundary to target space, and returns the metadata updater for target boundary.
     *
     * @param targetSpaceId the id of target range
     * @param boundary      the boundary of data to be migrated
     * @return the metadata updater of target boundary
     */
    IKVSpaceMetadataWriter migrateTo(String targetSpaceId, Boundary boundary);

    IKVSpaceMetadataWriter migrateFrom(String fromSpaceId, Boundary boundary);
}
