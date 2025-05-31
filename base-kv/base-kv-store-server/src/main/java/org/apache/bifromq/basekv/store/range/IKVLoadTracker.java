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

import com.google.protobuf.ByteString;
import java.util.Map;

public interface IKVLoadTracker {
    interface ILoadRecorder {
        long startNanos();

        /**
         * Get the kv io times
         *
         * @return the access times to kv engine
         */
        int getKVIOs();

        /**
         * Get the total time spent on io of kv engine
         *
         * @return the total time in nanos
         */
        long getKVIONanos();

        Map<ByteString, Long> keyDistribution();

        /**
         * The latency spent for accessing this key
         *
         * @param key          the accessed key
         * @param latencyNanos the nanos spent
         */
        void record(ByteString key, long latencyNanos);

        /**
         * The latency spent for other kv activity
         *
         * @param latencyNanos the nanos spent
         */
        void record(long latencyNanos);

        void stop();
    }

    /**
     * Start a recorder
     *
     * @return the recorder
     */
    ILoadRecorder start();
}
