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
 * Restore session with atomic visibility semantics.
 */
public interface IRestoreSession {
    /**
     * Put data into staging.
     */
    IRestoreSession put(ByteString key, ByteString value);

    /**
     * Update metadata in staging.
     */
    IRestoreSession metadata(ByteString metaKey, ByteString metaValue);

    /**
     * Atomically publish staging to be visible.
     */
    void done();

    /**
     * Abort this restore session and drop all staged data.
     */
    void abort();

    /**
     * Staged operations count.
     */
    int count();

    /**
     * Listener to be notified on flush.
     */
    interface FlushListener {
        void onFlush(int count, long bytes);
    }
}
