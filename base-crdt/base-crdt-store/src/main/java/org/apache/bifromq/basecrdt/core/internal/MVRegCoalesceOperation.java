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
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.replacements;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.singleValue;

import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.proto.Replacement;
import com.google.protobuf.ByteString;

class MVRegCoalesceOperation extends CoalesceOperation<IDotFunc, MVRegOperation> {
    MVRegOperation op;

    MVRegCoalesceOperation(ByteString replicaId, MVRegOperation op) {
        super(replicaId);
        coalesce(op);
    }

    @Override
    public void coalesce(MVRegOperation op) {
        this.op = op;
    }

    @Override
    public Iterable<Replacement> delta(IDotFunc current, IEventGenerator eventGenerator) {
        long ver = eventGenerator.nextEvent();
        switch (op.type) {
            case Write:
                return replacements(dot(replicaId, ver, singleValue(replicaId, ver, op.value)), current);
            case Reset:
            default:
                return replacements(dot(replicaId, ver), current);
        }
    }
}
