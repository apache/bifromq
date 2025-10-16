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

import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basekv.localengine.IKVSpaceEpoch;
import org.pcollections.HashPMap;
import org.pcollections.HashTreePMap;
import org.pcollections.TreePMap;

class InMemKVSpaceEpoch implements IKVSpaceEpoch {
    private final AtomicReference<HashPMap<ByteString, ByteString>> metadataMap;
    private final AtomicReference<TreePMap<ByteString, ByteString>> dataMap;

    InMemKVSpaceEpoch() {
        metadataMap = new AtomicReference<>(HashTreePMap.empty());
        dataMap = new AtomicReference<>(TreePMap.empty(unsignedLexicographicalComparator()));
    }

    InMemKVSpaceEpoch(InMemKVSpaceEpoch overlay) {
        metadataMap = new AtomicReference<>(overlay.metadataMap.get());
        dataMap = new AtomicReference<>(overlay.dataMap.get());
    }

    Map<ByteString, ByteString> metadataMap() {
        return metadataMap.get();
    }

    NavigableMap<ByteString, ByteString> dataMap() {
        return dataMap.get();
    }

    void setMetadata(ByteString key, ByteString value) {
        metadataMap.updateAndGet(m -> m.plus(key, value));
    }

    void removeMetadata(ByteString key) {
        metadataMap.updateAndGet(m -> m.minus(key));
    }

    void putData(ByteString key, ByteString value) {
        dataMap.updateAndGet(m -> m.plus(key, value));
    }

    void removeData(ByteString key) {
        dataMap.updateAndGet(m -> m.minus(key));
    }
}
