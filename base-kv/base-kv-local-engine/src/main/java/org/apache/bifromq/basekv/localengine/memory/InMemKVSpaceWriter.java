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

import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceMetadataWriter;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import org.slf4j.Logger;

class InMemKVSpaceWriter<E extends InMemKVEngine<E, T>, T extends InMemKVSpace<E, T>> extends InMemKVSpaceReader
    implements IKVSpaceWriter {
    private final Map<ByteString, ByteString> metadataMap;
    private final ConcurrentSkipListMap<ByteString, ByteString> rangeData;
    private final E engine;
    private final InMemKVSpaceWriterHelper helper;

    InMemKVSpaceWriter(String id,
                       Map<ByteString, ByteString> metadataMap,
                       ConcurrentSkipListMap<ByteString, ByteString> rangeData,
                       E engine,
                       ISyncContext syncContext,
                       Consumer<Boolean> afterWrite,
                       KVSpaceOpMeters readOpMeters,
                       Logger logger) {
        this(id, metadataMap, rangeData, engine, syncContext, new InMemKVSpaceWriterHelper(),
            afterWrite, readOpMeters, logger);
    }

    private InMemKVSpaceWriter(String id,
                               Map<ByteString, ByteString> metadataMap,
                               ConcurrentSkipListMap<ByteString, ByteString> rangeData,
                               E engine,
                               ISyncContext syncContext,
                               InMemKVSpaceWriterHelper writerHelper,
                               Consumer<Boolean> afterWrite,
                               KVSpaceOpMeters readOpMeters,
                               Logger logger) {
        super(id, readOpMeters, logger);
        this.metadataMap = metadataMap;
        this.rangeData = rangeData;
        this.engine = engine;
        this.helper = writerHelper;
        writerHelper.addMutators(id, metadataMap, rangeData, syncContext.mutator());
        writerHelper.addAfterWriteCallback(id, afterWrite);
    }


    @Override
    public IKVSpaceWriter metadata(ByteString metaKey, ByteString metaValue) {
        helper.metadata(id, metaKey, metaValue);
        return this;
    }

    @Override
    public IKVSpaceWriter insert(ByteString key, ByteString value) {
        helper.insert(id, key, value);
        return this;
    }

    @Override
    public IKVSpaceWriter put(ByteString key, ByteString value) {
        helper.put(id, key, value);
        return this;
    }

    @Override
    public IKVSpaceWriter delete(ByteString key) {
        helper.delete(id, key);
        return this;
    }

    @Override
    public IKVSpaceWriter clear() {
        helper.clear(id, Boundary.getDefaultInstance());
        return this;
    }

    @Override
    public IKVSpaceWriter clear(Boundary boundary) {
        helper.clear(id, boundary);
        return this;
    }

    @Override
    public IKVSpaceMetadataWriter migrateTo(String targetSpaceId, Boundary boundary) {
        try {
            InMemKVSpace<?, ?> targetKVSpace = engine.createIfMissing(targetSpaceId);
            IKVSpaceWriter targetKVSpaceWriter = targetKVSpace.toWriter();
            // move data
            try (IKVSpaceIterator itr = newIterator(boundary)) {
                for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                    targetKVSpaceWriter.put(itr.key(), itr.value());
                }
            }
            // clear moved data in left range
            helper.clear(id, boundary);
            return targetKVSpaceWriter;
        } catch (Throwable e) {
            throw new KVEngineException("Delete range in batch failed", e);
        }
    }

    @Override
    public IKVSpaceMetadataWriter migrateFrom(String fromSpaceId, Boundary boundary) {

        try {
            InMemKVSpace<?, ?> sourceKVSpace = engine.createIfMissing(fromSpaceId);
            IKVSpaceWriter sourceKVSpaceWriter = sourceKVSpace.toWriter();
            // move data
            try (IKVSpaceIterator itr = sourceKVSpace.newIterator(boundary)) {
                for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                    helper.put(id, itr.key(), itr.value());
                }
            }
            // clear moved data in right range
            sourceKVSpaceWriter.clear(boundary);
            return sourceKVSpaceWriter;
        } catch (Throwable e) {
            throw new KVEngineException("Delete range in batch failed", e);
        }
    }

    @Override
    public void done() {
        opMeters.batchWriteCallTimer.record(() -> {
            try {
                helper.done();
            } catch (Throwable e) {
                logger.error("Write Batch commit failed", e);
                throw new KVEngineException("Batch commit failed", e);
            }
        });
    }

    @Override
    public void abort() {
        helper.abort();

    }

    @Override
    public int count() {
        return helper.count();
    }

    @Override
    protected Map<ByteString, ByteString> metadataMap() {
        return metadataMap;
    }

    @Override
    protected ConcurrentSkipListMap<ByteString, ByteString> rangeData() {
        return rangeData;
    }
}
