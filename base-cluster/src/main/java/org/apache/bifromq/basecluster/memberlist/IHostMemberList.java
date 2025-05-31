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

package org.apache.bifromq.basecluster.memberlist;

import org.apache.bifromq.basecluster.memberlist.agent.IAgent;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.membership.proto.HostMember;
import io.reactivex.rxjava3.core.Observable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface IHostMemberList {
    /**
     * The member from local.
     *
     * @return the member object
     */
    HostMember local();

    boolean isZombie(HostEndpoint endpoint);

    /**
     * Quit local host from the member list, after quit the memberlist instance should never be used.
     */
    CompletableFuture<Void> stop();

    /**
     * The hot observable about members.
     *
     * @return the hot observable about members
     */
    Observable<Map<HostEndpoint, Integer>> members();

    /**
     * Host the provided agent in local host. If the agent is already hosted, nothing will happen, otherwise other hosts
     * which are residing same agent will get notified.
     *
     * @param agentId the agent id
     */
    IAgent host(String agentId);

    /**
     * Stop hosting the agent. If the agent is not a resident, nothing will happen. The agent object is not expected to
     * be used after calling this method.
     *
     * @param agentId the agent id
     */
    CompletableFuture<Void> stopHosting(String agentId);

    /**
     * The observable of agents landscape.
     *
     * @return the observable
     */
    Observable<Map<HostEndpoint, Set<String>>> landscape();
}
