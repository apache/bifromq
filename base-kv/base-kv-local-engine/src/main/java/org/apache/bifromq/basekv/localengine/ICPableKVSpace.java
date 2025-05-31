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

import java.util.Optional;

/**
 * KV space supports checkpoint.
 */
public interface ICPableKVSpace extends IKVSpace {
    /**
     * Make a checkpoint of the current range state.
     *
     * @return global unique id of the checkpoint
     */
    String checkpoint();

    /**
     * Open a readonly range object to access the checkpoint state. When the returned range object is garbage-collected
     * the associated checkpoint will be cleaned as well, except for the latest checkpoint. So the caller should keep a
     * strong reference to the checkpoint if it's still useful.
     *
     * @param checkpointId the checkpoint id
     * @return the range object for accessing the checkpoint
     */
    Optional<IKVSpaceCheckpoint> openCheckpoint(String checkpointId);
}
