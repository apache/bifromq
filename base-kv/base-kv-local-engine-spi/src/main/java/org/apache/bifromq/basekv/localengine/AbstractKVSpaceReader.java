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

package org.apache.bifromq.basekv.localengine;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.slf4j.Logger;

/**
 * The base implementation of IKVSpaceReader with operation metrics.
 */
public abstract class AbstractKVSpaceReader implements IKVSpaceReader {
    protected final String id;
    protected final KVSpaceOpMeters opMeters;
    protected final Logger logger;

    protected AbstractKVSpaceReader(String id, KVSpaceOpMeters opMeters, Logger logger) {
        this.id = id;
        this.opMeters = opMeters;
        this.logger = logger;
    }

    @Override
    public final String id() {
        return id;
    }

    @Override
    public final Optional<ByteString> metadata(ByteString metaKey) {
        return opMeters.metadataCallTimer.record(() -> doMetadata(metaKey));
    }

    @Override
    public final boolean exist(ByteString key) {
        return opMeters.existCallTimer.record(() -> doExist(key));
    }

    @Override
    public final Optional<ByteString> get(ByteString key) {
        return opMeters.getCallTimer.record(() -> doGet(key).map(k -> {
            opMeters.readBytesSummary.record(k.size());
            return k;
        }));
    }

    @Override
    public final IKVSpaceIterator newIterator() {
        return this.newIterator(Boundary.getDefaultInstance());
    }

    @Override
    public final IKVSpaceIterator newIterator(Boundary subBoundary) {
        return opMeters.iterNewCallTimer.record(
            () -> new MonitoredKeyRangeIterator(doNewIterator(subBoundary)));
    }

    @Override
    public final long size(Boundary boundary) {
        return opMeters.sizeCallTimer.record(() -> doSize(boundary));
    }

    protected abstract Optional<ByteString> doMetadata(ByteString metaKey);

    protected abstract boolean doExist(ByteString key);

    protected abstract Optional<ByteString> doGet(ByteString key);

    protected abstract IKVSpaceIterator doNewIterator(Boundary subBoundary);

    protected abstract long doSize(Boundary boundary);

    private class MonitoredKeyRangeIterator implements IKVSpaceIterator {
        final IKVSpaceIterator delegate;

        private MonitoredKeyRangeIterator(IKVSpaceIterator delegate) {
            this.delegate = delegate;
        }

        @Override
        public ByteString key() {
            ByteString key = delegate.key();
            opMeters.readBytesSummary.record(key.size());
            return key;
        }

        @Override
        public ByteString value() {
            ByteString value = delegate.value();
            opMeters.readBytesSummary.record(value.size());
            return value;
        }

        @Override
        public boolean isValid() {
            return delegate.isValid();
        }

        @Override
        public void next() {
            opMeters.iterNextCallTimer.record(delegate::next);
        }

        @Override
        public void prev() {
            opMeters.iterPrevCallTimer.record(delegate::prev);
        }

        @Override
        public void seekToFirst() {
            opMeters.iterSeekToFirstCallTimer.record(delegate::seekToFirst);
        }

        @Override
        public void seekToLast() {
            opMeters.iterSeekToLastCallTimer.record(delegate::seekToLast);
        }

        @Override
        public void seek(ByteString target) {
            opMeters.iterSeekCallTimer.record(() -> delegate.seek(target));
        }

        @Override
        public void seekForPrev(ByteString target) {
            opMeters.iterSeekForPrevCallTimer.record(() -> delegate.seekForPrev(target));
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
