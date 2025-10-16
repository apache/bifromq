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

import static org.apache.bifromq.basekv.localengine.memory.InMemKVHelper.sizeOfRange;

import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.bifromq.basekv.localengine.IKVSpaceIterator;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.slf4j.Logger;

class InMemKVSpaceCheckpointReader extends AbstractInMemKVSpaceReader {
    private final InMemKVSpaceEpoch checkpoint;

    protected InMemKVSpaceCheckpointReader(String id,
                                           KVSpaceOpMeters readOpMeters,
                                           Logger logger,
                                           InMemKVSpaceEpoch checkpoint) {
        super(id, readOpMeters, logger);
        this.checkpoint = checkpoint;
    }

    @Override
    protected Map<ByteString, ByteString> metadataMap() {
        return checkpoint.metadataMap();
    }

    @Override
    protected NavigableMap<ByteString, ByteString> rangeData() {
        return checkpoint.dataMap();
    }

    @Override
    protected IKVSpaceIterator doNewIterator(Boundary subBoundary) {
        return new InMemKVSpaceIterator(rangeData(), subBoundary);
    }

    @Override
    protected long doSize(Boundary boundary) {
        return sizeOfRange(checkpoint.dataMap(), boundary);
    }

    @Override
    public void close() {

    }
}
