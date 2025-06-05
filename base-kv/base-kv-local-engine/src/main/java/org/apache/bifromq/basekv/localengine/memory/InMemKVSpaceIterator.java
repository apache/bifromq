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

import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

class InMemKVSpaceIterator implements IKVSpaceIterator {
    private final ConcurrentSkipListMap<ByteString, ByteString> origData;
    private final Boundary boundary;
    private Map.Entry<ByteString, ByteString> currentEntry;
    private ConcurrentNavigableMap<ByteString, ByteString> dataSource;

    public InMemKVSpaceIterator(ConcurrentSkipListMap<ByteString, ByteString> data) {
        this(data, Boundary.getDefaultInstance());
    }

    public InMemKVSpaceIterator(ConcurrentSkipListMap<ByteString, ByteString> data, Boundary boundary) {
        origData = data;
        this.boundary = boundary;
        refresh();
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
        currentEntry = dataSource.higherEntry(currentEntry.getKey());
    }

    @Override
    public void prev() {
        currentEntry = dataSource.lowerEntry(currentEntry.getKey());
    }

    @Override
    public void seekToFirst() {
        currentEntry = dataSource.firstEntry();
    }

    @Override
    public void seekToLast() {
        currentEntry = dataSource.lastEntry();
    }

    @Override
    public void seek(ByteString target) {
        currentEntry = dataSource.ceilingEntry(target);
    }

    @Override
    public void seekForPrev(ByteString target) {
        currentEntry = dataSource.floorEntry(target);
    }

    @Override
    public void refresh() {
        ConcurrentSkipListMap<ByteString, ByteString> data = origData.clone();
        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
            dataSource = data;
        } else if (!boundary.hasStartKey()) {
            dataSource = data.headMap(boundary.getEndKey());
        } else if (!boundary.hasEndKey()) {
            dataSource = data.tailMap(boundary.getStartKey());
        } else {
            dataSource = data.subMap(boundary.getStartKey(), boundary.getEndKey());
        }
        currentEntry = dataSource.firstEntry();
    }

    @Override
    public void close() {
        currentEntry = null;
    }
}
