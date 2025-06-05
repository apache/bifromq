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
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;

/**
 * The interface of KV space, which is a logical range of key-value pairs.
 */
public interface IKVSpace extends IKVSpaceReader {
    Observable<Map<ByteString, ByteString>> metadata();

    KVSpaceDescriptor describe();

    /**
     * Open the space.
     */
    void open();

    /**
     * Close the space.
     */
    void close();

    /**
     * Destroy the range, after destroy all data and associated resources will be cleared and released. The range object
     * will transit to destroyed state.
     */
    void destroy();

    /**
     * Get a writer to update range state.
     *
     * @return the writer object
     */
    IKVSpaceWriter toWriter();
}
