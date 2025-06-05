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
import org.apache.bifromq.basecrdt.proto.Dot;
import org.apache.bifromq.basecrdt.proto.Replacement;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Set;
import java.util.stream.Collectors;

class CCounterCoalesceOperation extends CoalesceOperation<IDotMap, CCounterOperation> {
    private final Set<ByteString> zeroOutReplicaIds = Sets.newHashSet();
    private long inc;
    private boolean preset;

    CCounterCoalesceOperation(ByteString replicaId, CCounterOperation op) {
        super(replicaId);
        coalesce(op);
    }

    @Override
    public void coalesce(CCounterOperation op) {
        switch (op.type) {
            case Add -> inc += op.c;
            case Preset -> {
                if (op.replicaId == null) {
                    preset = true;
                    inc = op.c;
                } else {
                    zeroOutReplicaIds.add(op.replicaId);
                }
            }
            default -> throw new IllegalStateException("Unknown ccounter operation type: " + op.type);
        }
    }

    @Override
    public Iterable<Replacement> delta(IDotMap current, IEventGenerator eventGenerator) {
        long now = preset ? inc : getPartialCount(current.subDotFunc(replicaId).orElse(DotFunc.BOTTOM)) + inc;
        if (preset && inc == 0) {
            return zeroOut(eventGenerator.nextEvent(),
                current, Sets.union(zeroOutReplicaIds, Sets.<ByteString>newHashSet(replicaId)));
        }
        long ver = eventGenerator.nextEvent();
        if (zeroOutReplicaIds.isEmpty()) {
            return ProtoUtils.replacements(ProtoUtils.dot(replicaId, ver,
                    ProtoUtils.singleMap(replicaId,
                        ProtoUtils.singleValue(replicaId, ver, VarLong.encode(now)))),
                current.subDotFunc(replicaId).orElse(DotFunc.BOTTOM));
        }
        return Iterables.concat(ProtoUtils.replacements(
                ProtoUtils.dot(replicaId, ver, ProtoUtils.singleMap(replicaId,
                    ProtoUtils.singleValue(replicaId, ver, VarLong.encode(now)))),
                current.subDotFunc(replicaId).orElse(DotFunc.BOTTOM)),
            zeroOut(eventGenerator.nextEvent(), current, zeroOutReplicaIds));
    }

    private Iterable<Replacement> zeroOut(long ver, IDotMap current, Set<ByteString> replicaIds) {
        return ProtoUtils.replacements(ProtoUtils.dot(replicaId, ver), Iterables.concat(replicaIds.stream()
            .map(r -> current.subDotFunc(r).orElse(DotFunc.BOTTOM))
            .collect(Collectors.toSet())));
    }

    private long getPartialCount(IDotFunc df) {
        long maxVer = -1;
        long val = 0;
        for (Dot dot : df) {
            if (dot.getVer() > maxVer) {
                maxVer = dot.getVer();
                val = VarLong.decode(df.value(dot).get());
            }
        }
        return val;
    }
}
