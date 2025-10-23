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

import com.google.protobuf.ByteString;
import java.util.function.Consumer;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVEngineException;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.slf4j.Logger;

class InMemKVSpaceWriter<E extends InMemKVEngine<E, T>, T extends InMemKVSpace<E, T>> implements IKVSpaceWriter {
    protected final String id;
    protected final KVSpaceOpMeters opMeters;
    protected final Logger logger;
    protected final InMemKVSpaceWriterHelper helper;
    protected final E engine;
    protected final InMemKVSpaceEpoch epoch;

    InMemKVSpaceWriter(String id,
                       InMemKVSpaceEpoch epoch,
                       E engine,
                       ISyncContext syncContext,
                       Consumer<Boolean> afterWrite,
                       Consumer<InMemKVSpaceWriterHelper.WriteImpact> impactListener,
                       KVSpaceOpMeters readOpMeters,
                       Logger logger) {
        this(id, epoch, engine, syncContext, new InMemKVSpaceWriterHelper(),
            afterWrite, impactListener, readOpMeters, logger);
    }

    private InMemKVSpaceWriter(String id,
                               InMemKVSpaceEpoch epoch,
                               E engine,
                               ISyncContext syncContext,
                               InMemKVSpaceWriterHelper writerHelper,
                               Consumer<Boolean> afterWrite,
                               Consumer<InMemKVSpaceWriterHelper.WriteImpact> impactListener,
                               KVSpaceOpMeters readOpMeters,
                               Logger logger) {
        this.id = id;
        this.opMeters = readOpMeters;
        this.logger = logger;
        this.epoch = epoch;
        this.engine = engine;
        this.helper = writerHelper;
        writerHelper.addMutators(id, epoch, syncContext.mutator());
        writerHelper.addAfterWriteCallback(id, afterWrite);
        if (impactListener != null) {
            writerHelper.addAfterImpactCallback(id, impactListener);
        }
    }

    @Override
    public String id() {
        return id;
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
}
