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

import org.apache.bifromq.basecluster.messenger.proto.MessengerMessage;
import org.apache.bifromq.basecluster.transport.ITransport;
import org.apache.bifromq.basecluster.transport.PacketEnvelope;
import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Timed;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class MessengerTransport {

    private final ITransport transport;

    MessengerTransport(ITransport transport) {
        this.transport = transport;
    }

    InetSocketAddress bindAddress() {
        return transport.bindAddress();
    }

    CompletableFuture<Void> send(List<MessengerMessage> messengerMessages,
                                 InetSocketAddress recipient,
                                 boolean forceTCP) {
        ITransport.RELIABLE.set(forceTCP);
        return transport.send(messengerMessages.stream()
            .map(AbstractMessageLite::toByteString).collect(Collectors.toList()), recipient);
    }

    Observable<Timed<MessengerMessageEnvelope>> receive() {
        return transport.receive().flatMap(this::convert);
    }

    private Observable<Timed<MessengerMessageEnvelope>> convert(PacketEnvelope packetEnvelope) {
        return Observable.fromIterable(packetEnvelope.data.stream().map(b -> {
            MessengerMessageEnvelope.MessengerMessageEnvelopeBuilder messageEnvelopeBuilder =
                MessengerMessageEnvelope.builder()
                    .recipient(packetEnvelope.recipient);
            try {
                MessengerMessage mm = MessengerMessage.parseFrom(b);
                messageEnvelopeBuilder.message(mm);
                switch (mm.getMessengerMessageTypeCase()) {
                    case DIRECT:
                        return new Timed<MessengerMessageEnvelope>(
                            messageEnvelopeBuilder.sender(packetEnvelope.sender).build(),
                            System.currentTimeMillis(),
                            TimeUnit.MILLISECONDS);
                    case GOSSIP:
                    default:
                        return new Timed<MessengerMessageEnvelope>(
                            messageEnvelopeBuilder.build(),
                            System.currentTimeMillis(),
                            TimeUnit.MILLISECONDS);
                }
            } catch (InvalidProtocolBufferException e) {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList()));
    }
}
