/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basecrdt.store;

import static org.apache.bifromq.basecrdt.core.api.CRDTURI.checkURI;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import org.apache.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.UUID;

public class ReplicaIdGenerator {
    public static Replica generate(String crdtURI) {
        UUID uuid = UUID.randomUUID();
        return generate(crdtURI, unsafeWrap(ByteBuffer.allocate(2 * Long.BYTES)
            .putLong(uuid.getMostSignificantBits())
            .putLong(uuid.getLeastSignificantBits())
            .array()));
    }

    public static Replica generate(String crdtURI, ByteString id) {
        checkURI(crdtURI);
        return Replica.newBuilder()
            .setUri(crdtURI)
            .setId(id)
            .build();
    }
}
