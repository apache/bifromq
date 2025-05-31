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

package org.apache.bifromq.basecluster.memberlist.agent;

import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import org.apache.bifromq.basecluster.agent.proto.AgentMessage;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;

public interface IAgentMember {
    AgentMemberAddr address();

    /**
     * Broadcast a message among the agent members.
     *
     * @param message
     * @param reliable
     * @return
     */
    CompletableFuture<Void> broadcast(ByteString message, boolean reliable);

    /**
     * Send a message to another member located in given endpoint
     *
     * @param targetMemberAddr
     * @param message
     * @param reliable
     * @return
     */
    CompletableFuture<Void> send(AgentMemberAddr targetMemberAddr, ByteString message, boolean reliable);

    /**
     * Send a message to all endpoints where target member name is registered
     *
     * @param targetMemberName
     * @param message
     * @param reliable
     * @return
     */
    CompletableFuture<Void> multicast(String targetMemberName, ByteString message, boolean reliable);

    /**
     * Get current associated metadata
     *
     * @return
     */
    AgentMemberMetadata metadata();

    /**
     * Update associated metadata
     *
     * @param value
     */
    void metadata(ByteString value);

    /**
     * An observable of incoming messages
     *
     * @return
     */
    Observable<AgentMessage> receive();
}
