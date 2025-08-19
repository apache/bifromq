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

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberAddr;
import org.apache.bifromq.basecluster.agent.proto.AgentMemberMetadata;
import org.apache.bifromq.basecluster.agent.proto.AgentMessage;

public interface IAgentMember {
    AgentMemberAddr address();

    /**
     * Broadcast a message among the agent members.
     *
     * @param message the message to be sent
     * @param reliable if true, the message will be sent reliably, otherwise it may be dropped
     * @return a CompletableFuture that completes when the message is sent
     */
    CompletableFuture<Void> broadcast(ByteString message, boolean reliable);

    /**
     * Send a message to another member located in given endpoint.
     *
     * @param targetMemberAddr the address of the target member
     * @param message the message to be sent
     * @param reliable if true, the message will be sent reliably, otherwise it may be dropped
     * @return a CompletableFuture that completes when the message is sent
     */
    CompletableFuture<Void> send(AgentMemberAddr targetMemberAddr, ByteString message, boolean reliable);

    /**
     * Send a message to all endpoints where target member name is registered.
     *
     * @param targetMemberName the name of the target member
     * @param message the message to be sent
     * @param reliable if true, the message will be sent reliably, otherwise it may be dropped
     * @return a CompletableFuture that completes when the message is sent
     */
    CompletableFuture<Void> multicast(String targetMemberName, ByteString message, boolean reliable);

    /**
     * Get current associated metadata.
     *
     * @return the current metadata
     */
    AgentMemberMetadata metadata();

    /**
     * Update associated metadata.
     *
     * @param value the new metadata value
     */
    void metadata(ByteString value);

    /**
     * An observable of incoming messages.
     *
     * @return an observable that emits AgentMessage
     */
    Observable<AgentMessage> receive();

    /**
     * Refresh the registration of the local agent member.
     */
    void refresh();
}
