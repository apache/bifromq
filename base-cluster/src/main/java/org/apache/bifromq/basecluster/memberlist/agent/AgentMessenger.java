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
import org.apache.bifromq.basecluster.agent.proto.AgentMessage;
import org.apache.bifromq.basecluster.agent.proto.AgentMessageEnvelope;
import org.apache.bifromq.basecluster.memberlist.IHostAddressResolver;
import org.apache.bifromq.basecluster.messenger.IMessenger;
import org.apache.bifromq.basecluster.proto.ClusterMessage;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Timed;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

public class AgentMessenger implements IAgentMessenger {
    private final String agentId;
    private final IHostAddressResolver addressResolver;
    private final IMessenger messenger;

    public AgentMessenger(String agentId, IHostAddressResolver addressResolver, IMessenger messenger) {
        this.agentId = agentId;
        this.addressResolver = addressResolver;
        this.messenger = messenger;
    }

    @Override
    public CompletableFuture<Void> send(AgentMessage message, AgentMemberAddr receiver, boolean reliable) {
        InetSocketAddress memberAddress = addressResolver.resolve(receiver.getEndpoint());
        if (memberAddress != null) {
            return messenger.send(ClusterMessage.newBuilder()
                .setAgentMessage(AgentMessageEnvelope.newBuilder()
                    .setAgentId(agentId)
                    .setReceiver(receiver)
                    .setMessage(message)
                    .build())
                .build(), memberAddress, reliable);
        } else {
            // ignore temporary unreachable agent member
            return CompletableFuture.failedFuture(new UnknownHostException("Unknown host"));
        }
    }

    @Override
    public Observable<AgentMessageEnvelope> receive() {
        return messenger.receive()
            .map(Timed::value)
            .filter(msg -> msg.message.hasAgentMessage() &&
                msg.message.getAgentMessage().getAgentId().equals(agentId))
            .map(msg -> msg.message.getAgentMessage());

    }
}
