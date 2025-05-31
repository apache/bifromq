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

package org.apache.bifromq.basecrdt.core.internal;

import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.EWFlagOperation;
import org.apache.bifromq.basecrdt.core.api.IEWFlag;
import org.apache.bifromq.basecrdt.core.api.IEWFlagInflater;
import org.apache.bifromq.basecrdt.proto.Replica;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

class EWFlagInflater extends CausalCRDTInflater<IDotSet, EWFlagOperation, IEWFlag> implements IEWFlagInflater {
    EWFlagInflater(Replica replica, IReplicaStateLattice stateLattice,
                   ScheduledExecutorService executor, Duration inflationInterval, String... tags) {
        super(replica, stateLattice, executor, inflationInterval, tags);
    }

    @Override
    protected IEWFlag newCRDT(Replica replica, IDotSet dotStore,
                              CausalCRDT.CRDTOperationExecutor<EWFlagOperation> executor) {
        return new EWFlag(replica, () -> dotStore, executor);
    }

    @Override
    public CausalCRDTType type() {
        return CausalCRDTType.ewflag;
    }

    @Override
    protected ICoalesceOperation<IDotSet, EWFlagOperation> startCoalescing(EWFlagOperation op) {
        return new EWFlagCoalesceOperation(id().getId(), op);
    }

    @Override
    protected Class<? extends IDotSet> dotStoreType() {
        return DotSet.class;
    }
}
