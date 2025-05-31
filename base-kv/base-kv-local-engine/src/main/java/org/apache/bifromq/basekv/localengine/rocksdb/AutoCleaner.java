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

package org.apache.bifromq.basekv.localengine.rocksdb;

import java.lang.ref.Cleaner;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class AutoCleaner {
    private static final Cleaner CLEANER = Cleaner.create();

    public static <T extends AutoCloseable, O> T autoRelease(T object, O owner) {
        CLEANER.register(owner, new CloseableState(object));
        return object;
    }

    private record CloseableState(AutoCloseable state) implements Runnable {

        @Override
        public void run() {
            try {
                this.state.close();
            } catch (Exception e) {
                log.error("Failed to close object", e);
            }
        }
    }
}
