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

import org.apache.bifromq.basecrdt.core.api.DWFlagOperation;
import org.apache.bifromq.basecrdt.core.api.IDWFlag;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.proto.StateLattice;

class DWFlag extends CausalCRDT<IDotSet, DWFlagOperation> implements IDWFlag {
    private volatile boolean flag;

    DWFlag(Replica replica, DotStoreAccessor<IDotSet> dotStoreAccessor,
           CRDTOperationExecutor<DWFlagOperation> executor) {
        super(replica, dotStoreAccessor, executor);
        refresh();
    }

    @Override
    public boolean read() {
        return flag;
    }

    @Override
    protected void handleInflation(Iterable<StateLattice> addEvents, Iterable<StateLattice> removeEvents) {
        refresh();
    }

    private void refresh() {
        flag = dotStoreAccessor.fetch().isBottom();
    }
}
