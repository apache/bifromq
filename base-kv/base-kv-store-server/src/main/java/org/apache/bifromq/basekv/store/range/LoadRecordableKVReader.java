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

package org.apache.bifromq.basekv.store.range;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;

class LoadRecordableKVReader implements IKVRangeRefreshableReader {
    private final IKVRangeRefreshableReader delegate;
    private final IKVLoadRecorder recorder;

    LoadRecordableKVReader(IKVRangeRefreshableReader delegate, IKVLoadRecorder recorder) {
        this.delegate = delegate;
        this.recorder = recorder;
    }

    @Override
    public long version() {
        return delegate.version();
    }

    @Override
    public State state() {
        return delegate.state();
    }

    @Override
    public long lastAppliedIndex() {
        return delegate.lastAppliedIndex();
    }

    @Override
    public Boundary boundary() {
        return delegate.boundary();
    }

    @Override
    public ClusterConfig clusterConfig() {
        return delegate.clusterConfig();
    }

    @Override
    public long size(Boundary boundary) {
        return delegate.size(boundary);
    }

    @Override
    public boolean exist(ByteString key) {
        long start = System.nanoTime();
        boolean result = delegate.exist(key);
        recorder.record(key, System.nanoTime() - start);
        return result;
    }

    @Override
    public Optional<ByteString> get(ByteString key) {
        long start = System.nanoTime();
        Optional<ByteString> result = delegate.get(key);
        recorder.record(key, System.nanoTime() - start);
        return result;
    }

    @Override
    public IKVIterator iterator() {
        return new LoadRecordableKVIterator(delegate.iterator(), recorder);
    }

    @Override
    public IKVIterator iterator(Boundary boundary) {
        return new LoadRecordableKVIterator(delegate.iterator(boundary), recorder);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void refresh() {
        long start = System.nanoTime();
        delegate.refresh();
        recorder.record(System.nanoTime() - start);
    }
}
