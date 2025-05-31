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
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IMVRegInflater;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

class MVRegInflater extends CausalCRDTInflater<IDotFunc, MVRegOperation, IMVReg> implements IMVRegInflater {
    MVRegInflater(Replica replica,
                  IReplicaStateLattice stateLattice,
                  ScheduledExecutorService executor,
                  Duration inflationInterval,
                  String... tags) {
        super(replica, stateLattice, executor, inflationInterval, tags);
    }

    @Override
    protected IMVReg newCRDT(Replica replica, IDotFunc dotStore,
                             CausalCRDT.CRDTOperationExecutor<MVRegOperation> executor) {
        return new MVReg(replica, () -> dotStore, executor);
    }

    @Override
    public CausalCRDTType type() {
        return CausalCRDTType.mvreg;
    }

    @Override
    protected ICoalesceOperation<IDotFunc, MVRegOperation> startCoalescing(MVRegOperation op) {
        return new MVRegCoalesceOperation(id().getId(), op);
    }

    @Override
    protected Class<? extends IDotFunc> dotStoreType() {
        return DotFunc.class;
    }
}
