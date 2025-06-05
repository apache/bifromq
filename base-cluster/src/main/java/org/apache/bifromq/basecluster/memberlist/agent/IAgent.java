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

import org.apache.bifromq.basecluster.agent.proto.AgentEndpoint;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface IAgent {
    String id();

    /**
     * The local agent address.
     *
     * @return the local agent address
     */
    AgentEndpoint local();

    /**
     * A hot observable of agent membership.
     *
     * @return
     */
    Observable<Map<AgentMemberAddr, AgentMemberMetadata>> membership();

    /**
     * Register a local agent member. It's allowed to register same member name in same logical agent from different
     * agent hosts
     *
     * @param memberName
     */
    IAgentMember register(String memberName);

    /**
     * Deregister a member instance, the caller should never hold the reference to the instance after deregistered
     *
     * @param member
     */
    CompletableFuture<Void> deregister(IAgentMember member);
}
