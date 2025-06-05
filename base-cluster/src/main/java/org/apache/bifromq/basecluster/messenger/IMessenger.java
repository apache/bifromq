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

package org.apache.bifromq.basecluster.messenger;

import org.apache.bifromq.basecluster.proto.ClusterMessage;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Timed;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface IMessenger {
    InetSocketAddress bindAddress();
    CompletableFuture<Void> send(ClusterMessage message, InetSocketAddress recipient, boolean reliable);

    /**
     * send messages
     *
     * @param message
     * @param piggybackedGossips
     * @param recipient
     * @param reliable
     * @return
     */
    CompletableFuture<Void> send(ClusterMessage message,
                     List<ClusterMessage> piggybackedGossips,
                     InetSocketAddress recipient,
                     boolean reliable);

    /**
     * send messages
     *
     * @param message
     * @param piggybackedGossips
     * @param recipient
     * @param reliable
     * @return
     */
    CompletableFuture<Void> send(ClusterMessage message,
                     List<ClusterMessage> piggybackedGossips,
                     InetSocketAddress recipient,
                     String sender,
                     boolean reliable);

    /**
     * Spread the message in the cluster
     *
     * @param message
     * @return how long it takes to spread the message within the cluster
     */
    CompletableFuture<Duration> spread(ClusterMessage message);

    Observable<Timed<MessageEnvelope>> receive();

    void start(IRecipientSelector recipientSelector);

    void shutdown();
}
