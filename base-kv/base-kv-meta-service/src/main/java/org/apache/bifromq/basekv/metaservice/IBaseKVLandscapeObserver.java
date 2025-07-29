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

package org.apache.bifromq.basekv.metaservice;

import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.Optional;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;

/**
 * The landscape observer of base-kv cluster.
 */
public interface IBaseKVLandscapeObserver {
    /**
     * Get the observable of landscape.
     *
     * @return the observable of landscape
     */
    Observable<Map<String, KVRangeStoreDescriptor>> landscape();

    /**
     * Get the store descriptor for the given store id in current landscape.
     *
     * @param storeId the store id
     * @return the store descriptor
     */
    Optional<KVRangeStoreDescriptor> getStoreDescriptor(String storeId);

    /**
     * Stop the manager.
     *
     */
    void stop();
}
