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

import org.apache.bifromq.basekv.localengine.AbstractKVSpaceReader;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;

abstract class InMemKVSpaceReader extends AbstractKVSpaceReader {
    protected InMemKVSpaceReader(String id, KVSpaceOpMeters readOpMeters, Logger logger) {
        super(id, readOpMeters, logger);
    }

    protected abstract Map<ByteString, ByteString> metadataMap();

    protected abstract ConcurrentSkipListMap<ByteString, ByteString> rangeData();

    @Override
    protected Optional<ByteString> doMetadata(ByteString metaKey) {
        return Optional.ofNullable(metadataMap().get(metaKey));
    }

    @Override
    protected long doSize(Boundary boundary) {
        SortedMap<ByteString, ByteString> sizedData;
        ConcurrentSkipListMap<ByteString, ByteString> rangeData = rangeData();
        if (!boundary.hasStartKey() && !boundary.hasEndKey()) {
            sizedData = rangeData;
        } else if (!boundary.hasStartKey()) {
            sizedData = rangeData.headMap(boundary.getEndKey());
        } else if (!boundary.hasEndKey()) {
            sizedData = rangeData.tailMap(boundary.getStartKey());
        } else {
            sizedData = rangeData.subMap(boundary.getStartKey(), boundary.getEndKey());
        }
        // this may take a long time
        return sizedData.entrySet()
            .stream()
            .map(entry -> entry.getKey().size() + entry.getValue().size())
            .reduce(0, Integer::sum);
    }

    @Override
    protected boolean doExist(ByteString key) {
        return rangeData().containsKey(key);
    }

    @Override
    protected Optional<ByteString> doGet(ByteString key) {
        return Optional.ofNullable(rangeData().get(key));
    }

    @Override
    protected IKVSpaceIterator doNewIterator() {
        return new InMemKVSpaceIterator(rangeData());
    }

    @Override
    protected IKVSpaceIterator doNewIterator(Boundary subBoundary) {
        return new InMemKVSpaceIterator(rangeData(), subBoundary);
    }
}
