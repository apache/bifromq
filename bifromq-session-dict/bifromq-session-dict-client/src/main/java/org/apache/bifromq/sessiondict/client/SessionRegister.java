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

package org.apache.bifromq.sessiondict.client;

import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.baserpc.client.IRPCClient.IMessageStream;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.sessiondict.rpc.proto.Quit;
import org.apache.bifromq.sessiondict.rpc.proto.Session;
import org.apache.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import org.apache.bifromq.type.ClientInfo;

@Slf4j
class SessionRegister {
    private static final Cleaner CLEANER = Cleaner.create();

    private final IRPCClient.IMessageStream<Quit, Session> messageStream;
    private final Map<ClientInfo, Consumer<Quit>> sessions = new ConcurrentHashMap<>();
    private final Cleanable cleanable;

    SessionRegister(String tenantId, String registerKey, IRPCClient rpcClient) {
        this.messageStream = rpcClient.createMessageStream(tenantId, null, registerKey,
            Collections.emptyMap(), SessionDictServiceGrpc.getDictMethod());
        this.cleanable = CLEANER.register(this, new PipelineCloseAction(messageStream));
        this.messageStream.onMessage(new QuitListener(sessions));
        this.messageStream.onRetarget(new RetargetListener(sessions, messageStream));
    }

    public void sendRegInfo(ClientInfo owner, boolean keep) {
        messageStream.ack(Session.newBuilder()
            .setReqId(System.nanoTime())
            .setOwner(owner)
            .setKeep(keep)
            .build());
    }

    public void reg(ClientInfo owner, Consumer<Quit> kickConsumer) {
        sessions.put(owner, kickConsumer);
    }

    public void unreg(ClientInfo owner) {
        sessions.remove(owner);
    }

    public void close() {
        if (cleanable != null) {
            cleanable.clean();
        }
    }

    private record QuitListener(Map<ClientInfo, Consumer<Quit>> sessions) implements Consumer<Quit> {
        @Override
        public void accept(Quit quit) {
            sessions.computeIfPresent(quit.getOwner(), (k, v) -> {
                v.accept(quit);
                return v;
            });
        }
    }

    private record RetargetListener(Map<ClientInfo, Consumer<Quit>> sessions,
                                    IRPCClient.IMessageStream<Quit, Session> messageStream) implements Consumer<Long> {

        @Override
        public void accept(Long ts) {
            for (ClientInfo owner : sessions.keySet()) {
                this.messageStream.ack(Session.newBuilder()
                    .setReqId(System.nanoTime())
                    .setOwner(owner)
                    .setKeep(true)
                    .build());
            }
        }
    }

    private record PipelineCloseAction(IMessageStream<Quit, Session> messageStream) implements Runnable {
        @Override
        public void run() {
            messageStream.close();
        }
    }
}
