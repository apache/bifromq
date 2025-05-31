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

import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.dot;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.replacement;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.singleMap;

import org.apache.bifromq.basecrdt.core.api.RWORSetOperation;
import org.apache.bifromq.basecrdt.proto.Replacement;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Set;
import java.util.stream.Collectors;

class RWORSetCoalesceOperation extends CoalesceOperation<IDotMap, RWORSetOperation> {

    boolean clearAtFirst = false;
    private final Set<ByteString> adds = Sets.newConcurrentHashSet();
    private final Set<ByteString> rems = Sets.newConcurrentHashSet();

    RWORSetCoalesceOperation(ByteString replicaId, RWORSetOperation op) {
        super(replicaId);
        coalesce(op);
    }

    @Override
    public void coalesce(RWORSetOperation op) {
        if (op.type == RWORSetOperation.Type.Clear) {
            clearAtFirst = true;
            adds.clear();
            rems.clear();
        } else {
            switch (op.type) {
                case Add -> {
                    adds.add(op.element);
                    rems.remove(op.element);
                }
                case Remove -> {
                    adds.remove(op.element);
                    rems.add(op.element);
                }
            }
        }
    }

    @Override
    public Iterable<Replacement> delta(IDotMap current, IEventGenerator eventGenerator) {
        // DotMap<ByteString, DotMap<TRUE, DotSet>>
        Iterable<Replacement> addElements = adds.stream()
            .map(e -> {
                long ver = eventGenerator.nextEvent();
                return replacement(dot(replicaId, ver,
                    singleMap(e, singleMap(RWORSet.TRUE, ProtoUtils.singleDot(replicaId, ver)))));
            })
            .collect(Collectors.toSet());
        // DotMap<ByteString, DotMap<FALSE, DotSet>>
        Iterable<Replacement> remElements = rems.stream()
            .map(e -> {
                long ver = eventGenerator.nextEvent();
                return replacement(dot(replicaId, ver,
                    singleMap(e, singleMap(RWORSet.FALSE, ProtoUtils.singleDot(replicaId, ver)))));
            })
            .collect(Collectors.toSet());
        if (clearAtFirst) {
            return Iterables.concat(addElements, remElements,
                ProtoUtils.replacements(dot(replicaId, eventGenerator.nextEvent()), current));
        } else {
            return Iterables.concat(addElements, remElements,
                Iterables.transform(Iterables.concat(Sets.union(adds, rems)
                    .stream()
                    .map(e -> current.subDotMap(e).orElse(DotMap.BOTTOM))
                    .collect(Collectors.toSet())), ProtoUtils::replacement));
        }
    }
}

