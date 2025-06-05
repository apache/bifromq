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

package org.apache.bifromq.inbox.client;

import org.apache.bifromq.baserpc.client.IConnectable;
import org.apache.bifromq.inbox.rpc.proto.AttachReply;
import org.apache.bifromq.inbox.rpc.proto.AttachRequest;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.rpc.proto.DeleteReply;
import org.apache.bifromq.inbox.rpc.proto.DeleteRequest;
import org.apache.bifromq.inbox.rpc.proto.DetachReply;
import org.apache.bifromq.inbox.rpc.proto.DetachRequest;
import org.apache.bifromq.inbox.rpc.proto.ExistReply;
import org.apache.bifromq.inbox.rpc.proto.ExistRequest;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllReply;
import org.apache.bifromq.inbox.rpc.proto.ExpireAllRequest;
import org.apache.bifromq.inbox.rpc.proto.SendLWTReply;
import org.apache.bifromq.inbox.rpc.proto.SendLWTRequest;
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.rpc.proto.UnsubReply;
import org.apache.bifromq.inbox.rpc.proto.UnsubRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.plugin.subbroker.ISubBroker;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface IInboxClient extends ISubBroker, IConnectable, AutoCloseable {

    static InboxClientBuilder newBuilder() {
        return new InboxClientBuilder();
    }

    @Override
    default int id() {
        return 1;
    }

    CompletableFuture<ExistReply> exist(ExistRequest request);

    CompletableFuture<AttachReply> attach(AttachRequest request);

    CompletableFuture<DetachReply> detach(DetachRequest request);

    CompletableFuture<SubReply> sub(SubRequest request);

    CompletableFuture<UnsubReply> unsub(UnsubRequest request);

    CompletableFuture<SendLWTReply> sendLWT(SendLWTRequest request);

    CompletableFuture<DeleteReply> delete(DeleteRequest request);

    CompletableFuture<ExpireAllReply> expireAll(ExpireAllRequest request);

    IInboxReader openInboxReader(String tenantId, String inboxId, long incarnation);

    CompletableFuture<CommitReply> commit(CommitRequest request);

    void close();

    interface IInboxReader {
        void fetch(Consumer<Fetched> consumer);

        void hint(int bufferCapacity);

        void close();
    }
}
