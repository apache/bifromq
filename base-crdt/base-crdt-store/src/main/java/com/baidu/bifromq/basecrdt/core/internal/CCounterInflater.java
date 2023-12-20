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

package com.baidu.bifromq.basecrdt.core.internal;

import com.baidu.bifromq.basecrdt.core.api.CCounterOperation;
import com.baidu.bifromq.basecrdt.proto.Replica;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

class CCounterInflater extends CausalCRDTInflater<IDotMap, CCounterOperation, CCounter> {
    CCounterInflater(long id, Replica replica, IReplicaStateLattice stateLattice,
                     ScheduledExecutorService executor, Duration inflationInterval) {
        super(id, replica, stateLattice, executor, inflationInterval);
    }

    @Override
    protected CCounter newCRDT(Replica replica, IDotMap dotStore,
                               CausalCRDT.CRDTOperationExecutor<CCounterOperation> executor) {
        return new CCounter(replica, () -> dotStore, executor);
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
