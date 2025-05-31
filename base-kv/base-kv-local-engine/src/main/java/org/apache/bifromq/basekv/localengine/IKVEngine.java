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

import java.util.Map;

/**
 * The interface of kv engine.
 *
 * @param <T> the type of kv space created by the engine
 */
public interface IKVEngine<T extends IKVSpace> {

    String DEFAULT_NS = "default";

    /**
     * The unique identifier of the engine.
     *
     * @return id
     */
    String id();

    /**
     * Find all currently available kv spaces.
     *
     * @return the kv space list
     */
    Map<String, T> spaces();

    /**
     * Create a new key range with specified spaceId and boundary or get existing key range.
     *
     * @param spaceId the space id
     * @return the key range created
     */
    T createIfMissing(String spaceId);

    /**
     * Start the kv engine and specifying additional tags for generated metrics.
     *
     * @param metricTags the additional metric tags
     */
    void start(String... metricTags);

    /**
     * Stop the engine.
     */
    void stop();
}
