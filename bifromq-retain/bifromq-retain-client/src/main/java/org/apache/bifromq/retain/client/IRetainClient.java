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

package org.apache.bifromq.retain.client;

import org.apache.bifromq.baserpc.client.IConnectable;
import org.apache.bifromq.retain.rpc.proto.ExpireAllReply;
import org.apache.bifromq.retain.rpc.proto.ExpireAllRequest;
import org.apache.bifromq.retain.rpc.proto.MatchReply;
import org.apache.bifromq.retain.rpc.proto.MatchRequest;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import java.util.concurrent.CompletableFuture;

public interface IRetainClient extends IConnectable, AutoCloseable {
    static RetainClientBuilder newBuilder() {
        return new RetainClientBuilder();
    }

    CompletableFuture<MatchReply> match(MatchRequest request);

    CompletableFuture<RetainReply> retain(long reqId,
                                          String topic,
                                          QoS qos,
                                          ByteString payload,
                                          int expirySeconds,
                                          ClientInfo publisher);

    CompletableFuture<ExpireAllReply> expireAll(ExpireAllRequest request);

    void close();
}
