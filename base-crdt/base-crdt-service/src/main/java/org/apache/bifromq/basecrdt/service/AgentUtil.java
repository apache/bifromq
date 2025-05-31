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

package org.apache.bifromq.basecrdt.service;

import org.apache.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import java.util.Base64;
import lombok.SneakyThrows;

public class AgentUtil {
    @SneakyThrows
    public static Replica toReplica(String agentMemberName) {
        return Replica.parseFrom(fromBase64(agentMemberName));
    }

    public static String toAgentMemberName(Replica replica) {
        return toBase64(replica.toByteString());
    }

    private static String toBase64(ByteString bytes) {
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }

    private static ByteString fromBase64(String base64Str) {
        return ByteString.copyFrom(Base64.getDecoder().decode(base64Str));
    }
}
