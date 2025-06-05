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

package org.apache.bifromq.basecluster.memberlist;

import static org.apache.bifromq.basecrdt.core.api.CRDTURI.toURI;
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.ormap;

import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.membership.proto.HostMember;
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CRDTUtil {
    public static final String AGENT_HOST_MAP_URI = toURI(ormap, "AGENT_HOST_MAP");

    public static Optional<HostMember> getHostMember(IORMap hostListCRDT, HostEndpoint endpoint) {
        return parse(hostListCRDT.getMVReg(endpoint.toByteString()));
    }

    public static Iterator<HostMember> iterate(IORMap hostListCRDT) {
        return Iterators.transform(hostListCRDT.keys(), orMapkey -> parse(hostListCRDT.getMVReg(orMapkey.key())).get());
    }

    private static Optional<HostMember> parse(IMVReg value) {
        List<HostMember> agentHostNodes = Lists.newArrayList(Iterators.filter(
            Iterators.transform(value.read(), data -> {
                try {
                    return HostMember.parseFrom(data);
                } catch (InvalidProtocolBufferException e) {
                    log.error("Unable to parse agent host node", e);
                    // this exception should not happen
                    return null;
                }
            }), Objects::nonNull));
        agentHostNodes.sort((a, b) -> b.getIncarnation() - a.getIncarnation());
        return Optional.ofNullable(agentHostNodes.isEmpty() ? null : agentHostNodes.get(0));
    }
}
