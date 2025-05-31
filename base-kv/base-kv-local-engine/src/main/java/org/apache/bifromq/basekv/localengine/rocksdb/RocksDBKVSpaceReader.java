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

import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.DATA_SECTION_END;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.DATA_SECTION_START;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toDataKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compare;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.isValid;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.util.Collections.singletonList;
import static org.rocksdb.SizeApproximationFlag.INCLUDE_FILES;
import static org.rocksdb.SizeApproximationFlag.INCLUDE_MEMTABLES;

import org.apache.bifromq.basekv.localengine.AbstractKVSpaceReader;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.util.Optional;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Range;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.slf4j.Logger;

abstract class RocksDBKVSpaceReader extends AbstractKVSpaceReader {

    protected RocksDBKVSpaceReader(String id, KVSpaceOpMeters opMeters, Logger logger) {
        super(id, opMeters, logger);
    }

    protected abstract RocksDB db();

    protected abstract ColumnFamilyHandle cfHandle();

    protected abstract ISyncContext.IRefresher newRefresher();

    protected final long doSize(Boundary boundary) {
        byte[] start =
            !boundary.hasStartKey() ? DATA_SECTION_START : toDataKey(boundary.getStartKey().toByteArray());
        byte[] end =
            !boundary.hasEndKey() ? DATA_SECTION_END : toDataKey(boundary.getEndKey().toByteArray());
        if (compare(start, end) < 0) {
            try (Slice startSlice = new Slice(start); Slice endSlice = new Slice(end)) {
                Range range = new Range(startSlice, endSlice);
                return db().getApproximateSizes(cfHandle(), singletonList(range), INCLUDE_MEMTABLES, INCLUDE_FILES)[0];
            }
        }
        return 0;
    }

    @Override
    protected final boolean doExist(ByteString key) {
        return get(key).isPresent();
    }

    @Override
    protected final Optional<ByteString> doGet(ByteString key) {
        try {
            byte[] data = db().get(cfHandle(), toDataKey(key));
            return Optional.ofNullable(data == null ? null : unsafeWrap(data));
        } catch (RocksDBException rocksDBException) {
            throw new KVEngineException("Get failed", rocksDBException);
        }
    }

    @Override
    protected final IKVSpaceIterator doNewIterator() {
        return new RocksDBKVSpaceIterator(db(), cfHandle(), Boundary.getDefaultInstance(), newRefresher());
    }

    @Override
    protected final IKVSpaceIterator doNewIterator(Boundary subBoundary) {
        assert isValid(subBoundary);
        return new RocksDBKVSpaceIterator(db(), cfHandle(), subBoundary, newRefresher());
    }
}
