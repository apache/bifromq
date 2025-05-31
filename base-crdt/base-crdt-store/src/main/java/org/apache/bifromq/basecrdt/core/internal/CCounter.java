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


import org.apache.bifromq.basecrdt.core.api.CCounterOperation;
import org.apache.bifromq.basecrdt.core.api.ICCounter;
import org.apache.bifromq.basecrdt.proto.Dot;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.proto.StateLattice;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.Map;

class CCounter extends CausalCRDT<IDotMap, CCounterOperation> implements ICCounter {
    private volatile long read = 0;

    CCounter(Replica replica, DotStoreAccessor<IDotMap> dotStoreAccessor,
             CRDTOperationExecutor<CCounterOperation> executor) {
        super(replica, dotStoreAccessor, executor);
        refresh();
    }

    @Override
    public long read() {
        return read;
    }

    @Override
    protected void handleInflation(Iterable<StateLattice> addEvents, Iterable<StateLattice> removeEvents) {
        refresh();
    }

    private void refresh() {
        long total = 0;
        IDotMap dotMap = dotStoreAccessor.fetch();
        Iterator<ByteString> dotFuncKeys = dotMap.dotFuncKeys();
        while (dotFuncKeys.hasNext()) {
            ByteString dotFuncKey = dotFuncKeys.next();
            IDotFunc dotFunc = dotMap.subDotFunc(dotFuncKey).orElse(DotFunc.BOTTOM);
            Map<ByteString, Dot> dots = Maps.newHashMap();
            for (Dot dot : dotFunc) {
                if (!dots.containsKey(dot.getReplicaId())) {
                    total += VarLong.decode(dotFunc.value(dot).get());
                    dots.put(dot.getReplicaId(), dot);
                } else if (dots.get(dot.getReplicaId()).getVer() < dot.getVer()) {
                    total -= VarLong.decode(dotFunc.value(dots.get(dot.getReplicaId())).get());
                    total += VarLong.decode(dotFunc.value(dot).get());
                    dots.put(dot.getReplicaId(), dot);
                }
            }
        }
        read = total;
    }
}
