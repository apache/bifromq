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

package org.apache.bifromq.basekv.localengine.memory;

import org.apache.bifromq.basekv.localengine.IWALableKVSpace;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;

public class InMemWALableKVSpace extends InMemKVSpace<InMemWALableKVEngine, InMemWALableKVSpace>
    implements IWALableKVSpace {
    protected InMemWALableKVSpace(String id,
                                  InMemKVEngineConfigurator configurator,
                                  InMemWALableKVEngine engine,
                                  Runnable onDestroy,
                                  KVSpaceOpMeters opMeters,
                                  Logger logger) {
        super(id, configurator, engine, onDestroy, opMeters, logger);
    }

    @Override
    public CompletableFuture<Long> flush() {
        return CompletableFuture.completedFuture(System.nanoTime());
    }
}
