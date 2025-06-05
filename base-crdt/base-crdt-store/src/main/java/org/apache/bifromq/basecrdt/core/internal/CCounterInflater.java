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

package org.apache.bifromq.basecrdt.core.internal;

import org.apache.bifromq.basecrdt.core.api.CCounterOperation;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.ICCounter;
import org.apache.bifromq.basecrdt.core.api.ICCounterInflater;
import org.apache.bifromq.basecrdt.proto.Replica;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

class CCounterInflater extends CausalCRDTInflater<IDotMap, CCounterOperation, ICCounter> implements ICCounterInflater {
    CCounterInflater(Replica replica,
                     IReplicaStateLattice stateLattice,
                     ScheduledExecutorService executor,
                     Duration inflationInterval,
                     String... tags) {
        super(replica, stateLattice, executor, inflationInterval, tags);
    }

    @Override
    protected ICCounter newCRDT(Replica replica, IDotMap dotStore,
                                CausalCRDT.CRDTOperationExecutor<CCounterOperation> executor) {
        return new CCounter(replica, () -> dotStore, executor);
    }

    @Override
    public CausalCRDTType type() {
        return CausalCRDTType.cctr;
    }

    @Override
    protected ICoalesceOperation<IDotMap, CCounterOperation> startCoalescing(CCounterOperation op) {
        return new CCounterCoalesceOperation(id().getId(), op);
    }

    @Override
    protected Class<? extends IDotMap> dotStoreType() {
        return DotMap.class;
    }
}
