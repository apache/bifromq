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

package org.apache.bifromq.basekv.localengine.memory;

import static org.apache.bifromq.basekv.localengine.memory.InMemKVHelper.sizeOfRange;

import java.util.Collections;
import org.apache.bifromq.basekv.localengine.AbstractKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.SyncContext;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.slf4j.Logger;

abstract class InMemKVSpace<
    E extends InMemKVEngine<E, T>,
    T extends InMemKVSpace<E, T>> extends AbstractKVSpace<InMemKVSpaceEpoch> {
    protected final InMemKVEngineConfigurator configurator;
    protected final E engine;
    protected final ISyncContext syncContext = new SyncContext();
    protected final ISyncContext.IRefresher metadataRefresher = syncContext.refresher();

    protected InMemKVSpace(String id,
                           InMemKVEngineConfigurator configurator,
                           E engine,
                           Runnable onDestroy,
                           KVSpaceOpMeters opMeters,
                           Logger logger,
                           String... tags) {
        super(id, onDestroy, opMeters, logger, tags);
        this.configurator = configurator;
        this.engine = engine;
    }

    ISyncContext syncContext() {
        return syncContext;
    }

    @Override
    public IKVSpaceRefreshableReader reader() {
        return new InMemKVSpaceReader(id, opMeters, logger, syncContext.refresher(), this::handle);
    }

    protected void loadMetadata() {
        metadataRefresher.runIfNeeded((genBumped) -> {
            if (!handle().metadataMap().isEmpty()) {
                updateMetadata(Collections.unmodifiableMap(handle().metadataMap()));
            }
        });
    }

    protected long doSize(Boundary boundary) {
        return sizeOfRange(handle().dataMap(), boundary);
    }
}
