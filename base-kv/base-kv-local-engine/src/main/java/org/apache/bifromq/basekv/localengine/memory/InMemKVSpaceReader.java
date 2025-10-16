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

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.slf4j.Logger;

class InMemKVSpaceReader extends AbstractInMemKVSpaceReader implements IKVSpaceRefreshableReader {
    private final ISyncContext.IRefresher refresher;
    private final Supplier<InMemKVSpaceEpoch> epochSupplier;
    private final Set<InMemKVSpaceIterator> openedIterators = Sets.newConcurrentHashSet();
    private volatile InMemKVSpaceEpoch currentEpoch;

    InMemKVSpaceReader(String id,
                       KVSpaceOpMeters readOpMeters,
                       Logger logger,
                       ISyncContext.IRefresher refresher,
                       Supplier<InMemKVSpaceEpoch> epochSupplier) {
        super(id, readOpMeters, logger);
        this.refresher = refresher;
        this.epochSupplier = epochSupplier;
        this.currentEpoch = epochSupplier.get();
    }

    @Override
    protected Map<ByteString, ByteString> metadataMap() {
        return currentEpoch.metadataMap();
    }

    @Override
    protected NavigableMap<ByteString, ByteString> rangeData() {
        return currentEpoch.dataMap();
    }

    @Override
    public void close() {

    }

    @Override
    public void refresh() {
        refresher.runIfNeeded((genBumped) -> {
            currentEpoch = epochSupplier.get();
            openedIterators.forEach(itr -> itr.refresh(currentEpoch.dataMap()));
        });
    }

    @Override
    protected IKVSpaceIterator doNewIterator(Boundary subBoundary) {
        InMemKVSpaceIterator itr = new InMemKVSpaceIterator(rangeData(), subBoundary, openedIterators::remove);
        openedIterators.add(itr);
        return itr;
    }

    @Override
    protected long doSize(Boundary boundary) {
        return sizeOfRange(currentEpoch.dataMap(), boundary);
    }
}
