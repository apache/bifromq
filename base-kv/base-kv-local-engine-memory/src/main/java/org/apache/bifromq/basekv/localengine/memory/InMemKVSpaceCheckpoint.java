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

import org.apache.bifromq.basekv.localengine.IKVSpaceCheckpoint;
import org.apache.bifromq.basekv.localengine.IKVSpaceReader;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.slf4j.Logger;

class InMemKVSpaceCheckpoint implements IKVSpaceCheckpoint {
    private final String id;
    private final KVSpaceOpMeters opMeters;
    private final Logger logger;
    private final String cpId;
    private final InMemKVSpaceEpoch checkpoint;

    protected InMemKVSpaceCheckpoint(String id,
                                     String cpId,
                                     InMemKVSpaceEpoch checkpoint,
                                     KVSpaceOpMeters opMeters,
                                     Logger logger) {
        this.id = id;
        this.opMeters = opMeters;
        this.logger = logger;
        this.cpId = cpId;
        this.checkpoint = checkpoint;
    }

    @Override
    public String cpId() {
        return cpId;
    }

    @Override
    public IKVSpaceReader newReader() {
        return new InMemKVSpaceCheckpointReader(id, opMeters, logger, checkpoint);
    }
}
