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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.inRange;

import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVRangeReader;
import com.google.protobuf.ByteString;
import java.util.Optional;

public class KVCheckpointReader implements IKVCheckpointReader {
    private final IKVSpaceReader kvSpace;
    private final IKVRangeReader kvRangeReader;
    private volatile IKVSpaceIterator kvSpaceIterator;

    KVCheckpointReader(IKVSpaceReader kvSpace, IKVRangeReader reader) {
        this.kvSpace = kvSpace;
        this.kvRangeReader = reader;
    }

    @Override
    public Boundary boundary() {
        return kvRangeReader.boundary();
    }

    @Override
    public long size(Boundary boundary) {
        assert inRange(boundary, boundary());
        return kvRangeReader.size(boundary);
    }

    @Override
    public boolean exist(ByteString key) {
        assert inRange(key, boundary());
        return kvSpace.exist(key);
    }

    @Override
    public Optional<ByteString> get(ByteString key) {
        assert inRange(key, boundary());
        return kvSpace.get(key);
    }

    @Override
    public IKVCheckpointIterator iterator() {
        return new KVCheckpointDataIterator(kvSpace.newIterator());
    }

    @Override
    public void refresh() {
    }
}
