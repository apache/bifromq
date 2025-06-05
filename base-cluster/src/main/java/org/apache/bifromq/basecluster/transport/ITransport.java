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

package org.apache.bifromq.basecluster.transport;

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ITransport {
    ThreadLocal<Boolean> RELIABLE = new ThreadLocal<>();

    InetSocketAddress bindAddress();

    /**
     * Send messages to recipient
     *
     * @return
     */
    CompletableFuture<Void> send(List<ByteString> data, InetSocketAddress recipient);

    /**
     * HOT observable of incoming Packet wrapped in Envelop where sender and recipient address indicated
     *
     * @return
     */
    Observable<PacketEnvelope> receive();

    CompletableFuture<Void> shutdown();
}
