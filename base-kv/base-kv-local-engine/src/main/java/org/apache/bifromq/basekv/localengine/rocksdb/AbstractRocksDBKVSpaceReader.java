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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.apache.bifromq.basekv.localengine.rocksdb.Keys.toDataKey;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.apache.bifromq.basekv.localengine.AbstractKVSpaceReader;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

abstract class AbstractRocksDBKVSpaceReader extends AbstractKVSpaceReader {
    protected AbstractRocksDBKVSpaceReader(String id,
                                           KVSpaceOpMeters opMeters,
                                           Logger logger) {
        super(id, opMeters, logger);
    }

    protected abstract IRocksDBKVSpaceEpoch handle();

    protected abstract RocksDBSnapshot snapshot();

    protected final long doSize(Boundary boundary) {
        return RocksDBHelper.sizeOfBoundary(handle(), boundary);
    }

    @Override
    protected final boolean doExist(ByteString key) {
        return get(key).isPresent();
    }

    @Override
    protected final Optional<ByteString> doGet(ByteString key) {
        try (ReadOptions readOptions = new ReadOptions()) {
            readOptions.setSnapshot(snapshot().snapshot());
            IRocksDBKVSpaceEpoch dbHandle = handle();
            byte[] data = dbHandle.db().get(dbHandle.cf(), readOptions, toDataKey(key));
            return Optional.ofNullable(data == null ? null : unsafeWrap(data));
        } catch (RocksDBException rocksDBException) {
            throw new KVEngineException("Get failed", rocksDBException);
        }
    }
}
