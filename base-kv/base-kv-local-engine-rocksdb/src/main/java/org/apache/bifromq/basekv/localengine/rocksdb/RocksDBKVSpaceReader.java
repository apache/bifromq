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

package org.apache.bifromq.basekv.localengine.rocksdb;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.isValid;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceRefreshableReader;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.slf4j.Logger;

class RocksDBKVSpaceReader extends AbstractRocksDBKVSpaceReader implements IKVSpaceRefreshableReader {
    private final ISyncContext.IRefresher refresher;
    private final Supplier<IRocksDBKVSpaceEpoch> dbSupplier;
    private final Supplier<Map<ByteString, ByteString>> metadataSupplier;
    private final AtomicReference<RocksDBSnapshot> snapshot = new AtomicReference<>();
    private final Set<RocksDBKVSpaceIterator> openedIterators = Sets.newConcurrentHashSet();
    private final IteratorOptions iteratorOptions;

    RocksDBKVSpaceReader(String id,
                         KVSpaceOpMeters opMeters,
                         Logger logger,
                         ISyncContext.IRefresher refresher,
                         Supplier<IRocksDBKVSpaceEpoch> dbSupplier,
                         Supplier<Map<ByteString, ByteString>> metadataSupplier,
                         IteratorOptions iteratorOptions) {
        super(id, opMeters, logger);
        this.refresher = refresher;
        this.dbSupplier = dbSupplier;
        this.metadataSupplier = metadataSupplier;
        this.snapshot.set(RocksDBSnapshot.take(dbSupplier.get()));
        this.iteratorOptions = iteratorOptions;
    }

    @Override
    public void refresh() {
        refresher.runIfNeeded((genBumped) -> {
            snapshot.getAndSet(RocksDBSnapshot.take(dbSupplier.get())).release();
            openedIterators.forEach(itr -> itr.refresh(snapshot.get()));
        });
    }

    @Override
    public void close() {
        openedIterators.forEach(RocksDBKVSpaceIterator::close);
        RocksDBSnapshot oldSnapshot = snapshot.getAndSet(null);
        oldSnapshot.release();
    }

    @Override
    protected IRocksDBKVSpaceEpoch handle() {
        return dbSupplier.get();
    }

    @Override
    protected RocksDBSnapshot snapshot() {
        return snapshot.get();
    }

    @Override
    protected Optional<ByteString> doMetadata(ByteString metaKey) {
        return refresher.call(() -> {
            Map<ByteString, ByteString> metaMap = metadataSupplier.get();
            return Optional.ofNullable(metaMap.get(metaKey));
        });
    }

    @Override
    protected IKVSpaceIterator doNewIterator(Boundary subBoundary) {
        assert isValid(subBoundary);
        RocksDBKVSpaceIterator itr = new RocksDBKVSpaceIterator(snapshot(), subBoundary,
            openedIterators::remove, iteratorOptions);
        openedIterators.add(itr);
        return itr;
    }
}
