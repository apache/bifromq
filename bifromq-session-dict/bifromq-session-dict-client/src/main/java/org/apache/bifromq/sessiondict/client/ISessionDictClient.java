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

package org.apache.bifromq.sessiondict.client;

import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.baserpc.client.IConnectable;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckRequest;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.sessiondict.rpc.proto.GetReply;
import org.apache.bifromq.sessiondict.rpc.proto.GetRequest;
import org.apache.bifromq.sessiondict.rpc.proto.KillAllReply;
import org.apache.bifromq.sessiondict.rpc.proto.KillReply;
import org.apache.bifromq.sessiondict.rpc.proto.ServerRedirection;
import org.apache.bifromq.sessiondict.rpc.proto.SubReply;
import org.apache.bifromq.sessiondict.rpc.proto.SubRequest;
import org.apache.bifromq.sessiondict.rpc.proto.UnsubReply;
import org.apache.bifromq.sessiondict.rpc.proto.UnsubRequest;
import org.apache.bifromq.type.ClientInfo;

public interface ISessionDictClient extends IConnectable, AutoCloseable {

    static SessionDictClientBuilder newBuilder() {
        return new SessionDictClientBuilder();
    }

    ISessionRegistration reg(ClientInfo owner, IKillListener killListener);

    CompletableFuture<KillReply> kill(long reqId, String tenantId, String userId, String clientId, ClientInfo killer,
                                      ServerRedirection redirection);

    /**
     * Kill all sessions for a tenant.
     *
     * @param reqId the request id
     * @param tenantId the tenant id
     * @param userId the user id, can be null
     * @param killer the client info of the killer
     * @param redirection the server redirection to send the kill request to
     * @return a CompletableFuture of KillAllReply
     */
    CompletableFuture<KillAllReply> killAll(long reqId,
                                            String tenantId,
                                            String userId,
                                            ClientInfo killer,
                                            ServerRedirection redirection);

    CompletableFuture<GetReply> get(GetRequest request);

    CompletableFuture<OnlineCheckResult> exist(OnlineCheckRequest onlineCheckRequest);

    CompletableFuture<SubReply> sub(SubRequest request);

    CompletableFuture<UnsubReply> unsub(UnsubRequest request);

    void close();

    interface IKillListener {
        void onKill(ClientInfo killer, ServerRedirection redirection);
    }
}
