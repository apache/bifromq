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

/**
 * The interface of an iterator for a KV space.
 */
public interface IKVSpaceIterator extends AutoCloseable {
    /**
     * Get the key of the current entry.
     *
     * @return the key of the current entry
     */
    ByteString key();

    /**
     * Get the value of the current entry.
     *
     * @return the value of the current entry
     */
    ByteString value();

    /**
     * Check if the iterator is valid.
     *
     * @return true if the iterator is valid, false otherwise
     */
    boolean isValid();

    /**
     * move the iterator to the next entry.
     */
    void next();

    /**
     * move the iterator to the previous entry.
     */
    void prev();

    /**
     * seek to the first entry.
     */
    void seekToFirst();

    /**
     * seek to the last entry.
     */
    void seekToLast();

    /**
     * Seek to the first entry that is at or past target.
     *
     * @param target the target key
     */
    void seek(ByteString target);

    /**
     * Seek to the last entry that is at or before target.
     *
     * @param target the target key
     */
    void seekForPrev(ByteString target);

    /**
     * Close the iterator and release associated resources.
     */
    void close();
}
