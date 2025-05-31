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

import org.apache.bifromq.basecrdt.core.api.AWORSetOperation;
import org.apache.bifromq.basecrdt.core.api.CCounterOperation;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.DWFlagOperation;
import org.apache.bifromq.basecrdt.core.api.EWFlagOperation;
import org.apache.bifromq.basecrdt.core.api.ICRDTOperation;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.core.api.RWORSetOperation;
import org.apache.bifromq.basecrdt.proto.Dot;
import org.apache.bifromq.basecrdt.proto.Replacement;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class ORMapCoalesceOperation implements ICoalesceOperation<IDotMap, ORMapOperation> {
    private static class ValueOperation<T extends IDotStore, O extends ICRDTOperation> {
        ICoalesceOperation<T, O> op;
        boolean removeAtFirst;

        ValueOperation(ICoalesceOperation<T, O> op) {
            this.op = op;
        }

        ValueOperation(boolean removeAtFirst) {
            this.removeAtFirst = removeAtFirst;
        }

        ValueOperation(ICoalesceOperation<T, O> op, boolean removeAtFirst) {
            this.op = op;
            this.removeAtFirst = removeAtFirst;
        }
    }

    private final ByteString replicaId;
    private final Map<ByteString, ValueOperation<? extends IDotStore, ? extends ICRDTOperation>> valueOps =
        Maps.newConcurrentMap();

    ORMapCoalesceOperation(ByteString replicaId, ORMapOperation op) {
        this.replicaId = replicaId;
        coalesce(op);
    }

    private ORMapCoalesceOperation(ByteString replicaId) {
        this.replicaId = replicaId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<Replacement> delta(IDotMap current, IEventGenerator eventGenerator) {
        List<Iterable<Replacement>> dotItrs = Lists.newLinkedList();
        for (Map.Entry<ByteString, ValueOperation<?, ?>> entry : valueOps.entrySet()) {
            ByteString internalKey = entry.getKey();
            ValueOperation<? extends IDotStore, ? extends ICRDTOperation> valOp = entry.getValue();
            switch (ORMapUtil.getType(internalKey)) {
                case aworset, rworset, ormap, cctr -> {
                    if (valOp.removeAtFirst) {
                        Optional<IDotMap> valueDotMap = current.subDotMap(internalKey);
                        if (valueDotMap.isPresent() && !valueDotMap.get().isBottom()) {
                            dotItrs.add(ProtoUtils.replacements(ProtoUtils.dot(replicaId, eventGenerator.nextEvent()),
                                valueDotMap.get()));
                        }
                    }
                    if (valOp.op != null) {
                        IDotMap valueDotMap = valOp.removeAtFirst ?
                            DotMap.BOTTOM : current.subDotMap(internalKey).orElse(DotMap.BOTTOM);
                        dotItrs.add(Iterables.transform(
                            ((ValueOperation<IDotMap, ? extends ICRDTOperation>) valOp)
                                .op.delta(valueDotMap, eventGenerator),
                            replacement -> {
                                Dot dot = replacement.getDots(0);
                                if (dot.hasLattice()) {
                                    return replacement.toBuilder()
                                        .setDots(0, dot.toBuilder()
                                            .setLattice(ProtoUtils.singleMap(internalKey, dot.getLattice()))
                                            .build())
                                        .build();
                                } else {
                                    return replacement;
                                }
                            }));
                    }
                }
                case dwflag, ewflag -> {
                    if (valOp.removeAtFirst) {
                        Optional<IDotSet> valueDotSet = current.subDotSet(internalKey);
                        if (valueDotSet.isPresent() && !valueDotSet.get().isBottom()) {
                            dotItrs.add(ProtoUtils.replacements(ProtoUtils.dot(replicaId, eventGenerator.nextEvent()),
                                valueDotSet.get()));
                        }
                    }
                    if (valOp.op != null) {
                        IDotSet valueDotSet = valOp.removeAtFirst ?
                            DotSet.BOTTOM : current.subDotSet(internalKey).orElse(DotSet.BOTTOM);
                        dotItrs.add(Iterables.transform(
                            ((ValueOperation<IDotSet, ? extends ICRDTOperation>) valOp)
                                .op.delta(valueDotSet, eventGenerator),
                            replacement -> {
                                Dot dot = replacement.getDots(0);
                                if (dot.hasLattice()) {
                                    return replacement.toBuilder()
                                        .setDots(0, dot.toBuilder()
                                            .setLattice(ProtoUtils.singleMap(internalKey, dot.getLattice()))
                                            .build())
                                        .build();
                                } else {
                                    return replacement;
                                }
                            }));
                    }
                }
                case mvreg -> {
                    if (valOp.removeAtFirst) {
                        Optional<IDotFunc> valueDotFunc = current.subDotFunc(internalKey);
                        if (valueDotFunc.isPresent() && !valueDotFunc.get().isBottom()) {
                            dotItrs.add(ProtoUtils.replacements(ProtoUtils.dot(replicaId, eventGenerator.nextEvent()),
                                valueDotFunc.get()));
                        }
                    }
                    if (valOp.op != null) {
                        IDotFunc valueDotFunc = valOp.removeAtFirst ?
                            DotFunc.BOTTOM : current.subDotFunc(internalKey).orElse(DotFunc.BOTTOM);
                        dotItrs.add(Iterables.transform(
                            ((ValueOperation<IDotFunc, ? extends ICRDTOperation>) valOp)
                                .op.delta(valueDotFunc, eventGenerator),
                            replacement -> {
                                Dot dot = replacement.getDots(0);
                                if (dot.hasLattice()) {
                                    return replacement.toBuilder()
                                        .setDots(0, dot.toBuilder()
                                            .setLattice(ProtoUtils.singleMap(internalKey, dot.getLattice()))
                                            .build())
                                        .build();
                                } else {
                                    return replacement;
                                }
                            }));
                    }
                }
                default ->
                    throw new UnsupportedOperationException("Unknown crdt type: " + ORMapUtil.getType(internalKey));
            }
        }
        return Iterables.concat(dotItrs);
    }

    @Override
    public void coalesce(ORMapOperation op) {
        coalesce(0, op);
    }

    private void coalesce(int pathIdx, ORMapOperation op) {
        switch (op.type) {
            case UpdateKey -> {
                ORMapOperation.ORMapUpdate<?> update = (ORMapOperation.ORMapUpdate<?>) op;
                ByteString[] keyPath = update.keyPath;
                if (pathIdx < keyPath.length - 1) {
                    ByteString typedKey = toTypedKey(keyPath[pathIdx], op);
                    ((ORMapCoalesceOperation) valueOps
                        .computeIfAbsent(typedKey, k ->
                            new ValueOperation<>(new ORMapCoalesceOperation(replicaId))).op)
                        .coalesce(pathIdx + 1, op);
                } else {
                    ByteString typedKey = toTypedKey(keyPath[pathIdx], update.valueOp);
                    if (!valueOps.containsKey(typedKey)) {
                        valueOps.put(typedKey, new ValueOperation<>(startCoalesce(update.valueOp)));
                    } else {
                        if (valueOps.get(typedKey).op == null) {
                            valueOps.get(typedKey).op = startCoalesce(update.valueOp);
                        } else {
                            ((ICoalesceOperation) valueOps.get(typedKey).op).coalesce(update.valueOp);
                        }
                    }
                }
            }
            case RemoveKey -> {
                ORMapOperation.ORMapRemove remove = (ORMapOperation.ORMapRemove) op;
                ByteString[] keyPath = remove.keyPath;
                if (pathIdx < keyPath.length - 1) {
                    ByteString typedKey = toTypedKey(keyPath[pathIdx], op);
                    ValueOperation<?, ?> pathValOperation = valueOps.computeIfAbsent(typedKey,
                        k -> new ValueOperation<>(new ORMapCoalesceOperation(replicaId), false));
                    ((ORMapCoalesceOperation) pathValOperation.op).coalesce(pathIdx + 1, op);
                } else {
                    ByteString typedKey = ORMapUtil.typedKey(keyPath[pathIdx], remove.valueType);
                    if (!valueOps.containsKey(typedKey)) {
                        valueOps.put(typedKey, new ValueOperation<>(true));
                    } else {
                        valueOps.get(typedKey).removeAtFirst = true;
                        // remove descendant operations
                        valueOps.get(typedKey).op = null;
                    }
                }
            }
        }
    }

    private CausalCRDTType ofType(ICRDTOperation op) {
        if (op instanceof AWORSetOperation) {
            return CausalCRDTType.aworset;
        }
        if (op instanceof CCounterOperation) {
            return CausalCRDTType.cctr;
        }
        if (op instanceof DWFlagOperation) {
            return CausalCRDTType.dwflag;
        }
        if (op instanceof EWFlagOperation) {
            return CausalCRDTType.ewflag;
        }
        if (op instanceof RWORSetOperation) {
            return CausalCRDTType.rworset;
        }
        if (op instanceof ORMapOperation) {
            return CausalCRDTType.ormap;
        }
        if (op instanceof MVRegOperation) {
            return CausalCRDTType.mvreg;
        }
        throw new UnsupportedOperationException("Unknown operation type");
    }

    private ByteString toTypedKey(ByteString key, ICRDTOperation op) {
        return ORMapUtil.typedKey(key, ofType(op));
    }

    private ICoalesceOperation startCoalesce(ICRDTOperation op) {
        if (op instanceof AWORSetOperation) {
            return new AWORSetCoalesceOperation(replicaId, (AWORSetOperation) op);
        }
        if (op instanceof CCounterOperation) {
            return new CCounterCoalesceOperation(replicaId, (CCounterOperation) op);
        }
        if (op instanceof DWFlagOperation) {
            return new DWFlagCoalesceOperation(replicaId, (DWFlagOperation) op);
        }
        if (op instanceof EWFlagOperation) {
            return new EWFlagCoalesceOperation(replicaId, (EWFlagOperation) op);
        }
        if (op instanceof RWORSetOperation) {
            return new RWORSetCoalesceOperation(replicaId, (RWORSetOperation) op);
        }
        if (op instanceof ORMapOperation) {
            return new ORMapCoalesceOperation(replicaId, (ORMapOperation) op);
        }
        if (op instanceof MVRegOperation) {
            return new MVRegCoalesceOperation(replicaId, (MVRegOperation) op);
        }
        throw new UnsupportedOperationException("Unknown operation type");
    }

}
