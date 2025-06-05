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

import org.apache.bifromq.basecrdt.core.api.AWORSetOperation;
import org.apache.bifromq.basecrdt.proto.Replacement;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Set;
import java.util.stream.Collectors;

class AWORSetCoalesceOperation extends CoalesceOperation<IDotMap, AWORSetOperation> {
    private boolean clearAtFirst = false;
    private final Set<ByteString> adds = Sets.newConcurrentHashSet();
    private final Set<ByteString> rems = Sets.newConcurrentHashSet();

    AWORSetCoalesceOperation(ByteString replicaId, AWORSetOperation op) {
        super(replicaId);
        coalesce(op);
    }

    @Override
    public void coalesce(AWORSetOperation op) {
        if (op.type == AWORSetOperation.Type.Clear) {
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
        Iterable<Replacement> addDots = adds.stream().map(e -> {
                long ver = eventGenerator.nextEvent();
                return ProtoUtils.replacement(ProtoUtils.dot(replicaId, ver,
                    ProtoUtils.singleMap(e, ProtoUtils.singleDot(replicaId, ver))));
            })
            .collect(Collectors.toSet());
        if (clearAtFirst && !current.isBottom()) {
            return Iterables.concat(addDots,
                ProtoUtils.replacements(ProtoUtils.dot(replicaId, eventGenerator.nextEvent()), current));
        } else {
            return Iterables.concat(addDots, Iterables.transform(Iterables.concat(rems.stream()
                .map(e -> current.subDotSet(e).orElse(DotSet.BOTTOM))
                .collect(Collectors.toSet())), ProtoUtils::replacement));
        }
    }
}

