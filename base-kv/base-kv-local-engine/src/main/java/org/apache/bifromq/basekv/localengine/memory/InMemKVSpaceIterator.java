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
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.proto.Boundary;

class InMemKVSpaceIterator implements IKVSpaceIterator {
    private final Boundary boundary;
    private final AtomicReference<NavigableMap<ByteString, ByteString>> currentEpoch = new AtomicReference<>();
    private final CloseListener closeListener;
    private Map.Entry<ByteString, ByteString> currentEntry;
    private NavigableMap<ByteString, ByteString> dataSource;

    public InMemKVSpaceIterator(NavigableMap<ByteString, ByteString> epoch, Boundary boundary) {
        this(epoch, boundary, iterator -> {
        });
    }

    public InMemKVSpaceIterator(NavigableMap<ByteString, ByteString> epoch,
                                Boundary boundary,
                                CloseListener closeListener) {
        this.boundary = boundary;
        this.closeListener = closeListener;
        refresh(epoch);
    }

    @Override
    public ByteString key() {
        return currentEntry.getKey();
    }

    @Override
    public ByteString value() {
        return currentEntry.getValue();
    }

    @Override
    public boolean isValid() {
        return currentEntry != null;
    }

    @Override
    public void next() {
        currentEntry = dataSource().higherEntry(currentEntry.getKey());
    }

    @Override
    public void prev() {
        currentEntry = dataSource().lowerEntry(currentEntry.getKey());
    }

    @Override
    public void seekToFirst() {
        if (dataSource().isEmpty()) {
            currentEntry = null;
            return;
        }
        currentEntry = dataSource().firstEntry();
    }

    @Override
    public void seekToLast() {
        if (dataSource().isEmpty()) {
            currentEntry = null;
            return;
        }
        currentEntry = dataSource().lastEntry();
    }

    @Override
    public void seek(ByteString target) {
        currentEntry = dataSource().ceilingEntry(target);
    }

    @Override
    public void seekForPrev(ByteString target) {
        currentEntry = dataSource().floorEntry(target);
    }

    public void refresh(NavigableMap<ByteString, ByteString> epoch) {
        currentEntry = null;
        currentEpoch.set(epoch);
        NavigableMap<ByteString, ByteString> data = currentEpoch.get();
        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
            dataSource = data;
        } else if (!boundary.hasStartKey()) {
            dataSource = data.headMap(boundary.getEndKey(), false);
        } else if (!boundary.hasEndKey()) {
            dataSource = data.tailMap(boundary.getStartKey(), true);
        } else {
            dataSource = data.subMap(boundary.getStartKey(), true, boundary.getEndKey(), false);
        }
    }

    private NavigableMap<ByteString, ByteString> dataSource() {
        return dataSource;
    }

    @Override
    public void close() {
        currentEntry = null;
        closeListener.onClose(this);
    }

    interface CloseListener {
        void onClose(InMemKVSpaceIterator iterator);
    }
}
