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

package org.apache.bifromq.basecluster.memberlist.agent;

import static org.apache.bifromq.basecrdt.core.api.CRDTURI.toURI;
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.mvreg;
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.ormap;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IORMap;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class CRDTUtil {
    private static final String AGENTKEY_PREFIX = "A";

    static String toAgentURI(String agentId) {
        return toURI(ormap, AGENTKEY_PREFIX.concat(agentId));
    }

    static Map<AgentMemberAddr, AgentMemberMetadata> toAgentMemberMap(IORMap agentCRDT) {
        Map<AgentMemberAddr, AgentMemberMetadata> agentMemberMap = new HashMap<>();
        Iterator<IORMap.ORMapKey> orMapKeyItr = agentCRDT.keys();
        while (orMapKeyItr.hasNext()) {
            IORMap.ORMapKey orMapKey = orMapKeyItr.next();
            Optional<AgentMemberMetadata> meta = parseMetadata(agentCRDT.getMVReg(orMapKey.key()));
            meta.ifPresent(m -> agentMemberMap.put(parseAgentMemberAddr(orMapKey), m));
        }
        return agentMemberMap;
    }

    static Optional<AgentMemberMetadata> getAgentMemberMetadata(IORMap agentCRDT, AgentMemberAddr addr) {
        return parseMetadata(agentCRDT.getMVReg(addr.toByteString()));
    }

    @SneakyThrows
    private static AgentMemberAddr parseAgentMemberAddr(IORMap.ORMapKey key) {
        assert key.valueType() == mvreg;
        return AgentMemberAddr.parseFrom(key.key());
    }

    private static Optional<AgentMemberMetadata> parseMetadata(IMVReg value) {
        List<AgentMemberMetadata> metaList = Lists.newArrayList(Iterators.filter(
            Iterators.transform(value.read(), data -> {
                try {
                    return AgentMemberMetadata.parseFrom(data);
                } catch (InvalidProtocolBufferException e) {
                    log.error("Unable to parse agent host node", e);
                    // should not happen, skip the broken value
                    return null;
                }
            }), Objects::nonNull));
        metaList.sort(Comparator.comparingLong(AgentMemberMetadata::getHlc).reversed());
        return Optional.ofNullable(metaList.isEmpty() ? null : metaList.get(0));
    }
}
