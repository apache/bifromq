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

package org.apache.bifromq.basekv.store.range;

import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVReader;
import com.google.protobuf.ByteString;
import java.util.Optional;

class LoadRecordableKVReader implements IKVReader {
    private final IKVReader delegate;
    private final IKVLoadRecorder recorder;

    LoadRecordableKVReader(IKVReader delegate, IKVLoadRecorder recorder) {
        this.delegate = delegate;
        this.recorder = recorder;
    }

    @Override
    public Boundary boundary() {
        return delegate.boundary();
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
    public void refresh() {
        long start = System.nanoTime();
        delegate.refresh();
        recorder.record(System.nanoTime() - start);
    }
}
