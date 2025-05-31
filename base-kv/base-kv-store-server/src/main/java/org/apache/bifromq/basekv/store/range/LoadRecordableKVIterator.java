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

import org.apache.bifromq.basekv.store.api.IKVIterator;
import com.google.protobuf.ByteString;

public class LoadRecordableKVIterator implements IKVIterator {
    private final IKVIterator delegate;
    private final IKVLoadRecorder recorder;

    public LoadRecordableKVIterator(IKVIterator delegate, IKVLoadRecorder recorder) {
        this.delegate = delegate;
        this.recorder = recorder;
    }

    @Override
    public ByteString key() {
        long start = System.nanoTime();
        ByteString result = delegate.key();
        recorder.record(result, System.nanoTime() - start);
        return result;
    }

    @Override
    public ByteString value() {
        long start = System.nanoTime();
        ByteString result = delegate.value();
        recorder.record(System.nanoTime() - start);
        return result;
    }

    @Override
    public boolean isValid() {
        return delegate.isValid();
    }

    @Override
    public void next() {
        long start = System.nanoTime();
        delegate.next();
        recorder.record(System.nanoTime() - start);
    }

    @Override
    public void prev() {
        long start = System.nanoTime();
        delegate.prev();
        recorder.record(System.nanoTime() - start);
    }

    @Override
    public void seekToFirst() {
        long start = System.nanoTime();
        delegate.seekToFirst();
        recorder.record(System.nanoTime() - start);
    }

    @Override
    public void seekToLast() {
        long start = System.nanoTime();
        delegate.seekToLast();
        recorder.record(System.nanoTime() - start);
    }

    @Override
    public void seek(ByteString key) {
        long start = System.nanoTime();
        delegate.seek(key);
        recorder.record(key, System.nanoTime() - start);
    }

    @Override
    public void seekForPrev(ByteString key) {
        long start = System.nanoTime();
        delegate.seekForPrev(key);
        recorder.record(key, System.nanoTime() - start);
    }
}
