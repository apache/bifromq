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
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.store.api.IKVIterator;

class KVIterator implements IKVIterator {
    private final IKVSpaceIterator kvSpaceIterator;

    KVIterator(IKVSpaceIterator kvSpaceIterator) {
        this.kvSpaceIterator = kvSpaceIterator;
    }

    @Override
    public ByteString key() {
        return kvSpaceIterator.key();
    }

    @Override
    public ByteString value() {
        return kvSpaceIterator.value();
    }

    @Override
    public boolean isValid() {
        return kvSpaceIterator.isValid();
    }

    @Override
    public void next() {
        kvSpaceIterator.next();
    }

    @Override
    public void prev() {
        kvSpaceIterator.prev();
    }

    @Override
    public void seekToFirst() {
        kvSpaceIterator.seekToFirst();
    }

    @Override
    public void seekToLast() {
        kvSpaceIterator.seekToLast();
    }

    @Override
    public void seek(ByteString key) {
        kvSpaceIterator.seek(key);
    }

    @Override
    public void seekForPrev(ByteString key) {
        kvSpaceIterator.seekForPrev(key);
    }

    @Override
    public void close() {
        kvSpaceIterator.close();
    }
}
