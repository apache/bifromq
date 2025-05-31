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

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static java.util.Collections.singleton;

import org.apache.bifromq.basecrdt.proto.Dot;
import org.apache.bifromq.basecrdt.proto.Replacement;
import org.apache.bifromq.basecrdt.proto.SingleDot;
import org.apache.bifromq.basecrdt.proto.SingleMap;
import org.apache.bifromq.basecrdt.proto.SingleValue;
import org.apache.bifromq.basecrdt.proto.StateLattice;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;

class ProtoUtils {
    static ByteString toByteString(long l) {
        return unsafeWrap(ByteBuffer.allocate(Long.BYTES).putLong(l).array());
    }

    static Dot dot(ByteString replicaId, long ver) {
        return Dot.newBuilder().setReplicaId(replicaId).setVer(ver).build();
    }

    static Dot dot(ByteString replicaId, long ver, StateLattice lattice) {
        return Dot.newBuilder().setReplicaId(replicaId).setVer(ver).setLattice(lattice).build();
    }

    static Replacement replacement(Dot... dots) {
        Replacement.Builder builder = Replacement.newBuilder();
        for (Dot dot : dots) {
            builder.addDots(dot);
        }
        return builder.build();
    }

    static Iterable<Replacement> replacements(Dot dot, Iterable<Dot> replacingDots) {
        return replacingDots.iterator().hasNext() ?
            Iterables.transform(replacingDots, replacingDot -> replacement(dot, replacingDot)) :
            singleton(replacement(dot));
    }

    static StateLattice singleDot(ByteString replicaId, long ver) {
        return StateLattice.newBuilder()
            .setSingleDot(SingleDot.newBuilder()
                .setReplicaId(replicaId)
                .setVer(ver)
                .build())
            .build();
    }

    static StateLattice singleValue(ByteString replicaId, long ver, ByteString val) {
        return StateLattice.newBuilder()
            .setSingleValue(SingleValue.newBuilder()
                .setReplicaId(replicaId)
                .setVer(ver)
                .setValue(val)
                .build())
            .build();
    }

    static StateLattice singleMap(ByteString key, StateLattice val) {
        return StateLattice.newBuilder().setSingleMap(SingleMap.newBuilder().setKey(key).setVal(val).build()).build();
    }
}
