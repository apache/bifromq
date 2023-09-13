/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.basecluster.messenger;

import com.baidu.bifromq.basecluster.messenger.proto.DirectMessage;
import com.baidu.bifromq.basecluster.messenger.proto.GossipMessage;
import com.baidu.bifromq.basecluster.messenger.proto.MessengerMessage;
import com.baidu.bifromq.basecluster.proto.ClusterMessage;
import com.baidu.bifromq.basecluster.transport.Transport;
import com.baidu.bifromq.basecluster.util.RandomUtils;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.netty.handler.ssl.SslContext;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Messenger implements IMessenger {
    private enum State {
        INIT, START, STOP
    }

    private State state = State.INIT;
    // threshold for determine which transport to use
    private final MessengerTransport transport;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final InetSocketAddress localAddress;
    private final Gossiper gossiper;
    private final Subject<Timed<MessageEnvelope>> publisher =
        PublishSubject.<Timed<MessageEnvelope>>create().toSerialized();

    private final Scheduler scheduler;
    private final MessengerOptions opts;
    private final MetricManager metricManager;

    @Builder
    private Messenger(InetSocketAddress bindAddr,
                      SslContext serverSslContext,
                      SslContext clientSslContext,
                      String env,
                      Scheduler scheduler,
                      MessengerOptions opts) {
        env = env == null ? "" : env;
        this.opts = opts.toBuilder().build();
        this.scheduler = scheduler;
        this.transport = new MessengerTransport(Transport.builder()
            .bindAddr(bindAddr)
            .serverSslContext(serverSslContext)
            .clientSslContext(clientSslContext)
            .env(env)
            .options(opts.transporterOptions())
            .build());
        this.localAddress = transport.bindAddress();
        this.gossiper = new Gossiper(transport.bindAddress().toString(),
            opts.retransmitMultiplier(),
            opts.spreadPeriod(),
            this.scheduler);
        this.metricManager = new MetricManager(localAddress);
    }

    @Override
    public InetSocketAddress bindAddress() {
        return transport.bindAddress();
    }

    @Override
    public CompletableFuture<Void> send(ClusterMessage message, InetSocketAddress recipient, boolean reliable) {
        log.trace("Sending message: addr={}, reliable={}, message={}", recipient, reliable, message);
        metricManager.msgSendCounters.get(message.getClusterMessageTypeCase()).increment();
        DirectMessage directMessage = DirectMessage.newBuilder().setPayload(message.toByteString()).build();
        MessengerMessage messengerMessage = MessengerMessage.newBuilder().setDirect(directMessage).build();
        return transport.send(List.of(messengerMessage), recipient, reliable);
    }

    @Override
    public CompletableFuture<Void> send(ClusterMessage message,
                                        List<ClusterMessage> piggybackedGossips,
                                        InetSocketAddress recipient,
                                        boolean reliable) {
        return send(message, piggybackedGossips, recipient, "", reliable);
    }

    @Override
    public CompletableFuture<Void> send(ClusterMessage message,
                                        List<ClusterMessage> piggybackedGossips,
                                        InetSocketAddress recipient,
                                        String sender,
                                        boolean reliable) {
        log.trace("Sending message with piggyback: addr={}, reliable={}, message={}", recipient, reliable, message);
        metricManager.msgSendCounters.get(message.getClusterMessageTypeCase()).increment();
        List<MessengerMessage> buffer = new ArrayList<>();
        buffer.add(MessengerMessage.newBuilder()
            .setDirect(DirectMessage.newBuilder().setPayload(message.toByteString()).build())
            .build());
        piggybackedGossips.forEach(gossip -> {
            metricManager.msgSendCounters.get(message.getClusterMessageTypeCase()).increment();
            buffer.add(MessengerMessage.newBuilder()
                .setGossip(GossipMessage.newBuilder().setPayload(gossip.toByteString()).build())
                .build());
        });
        return transport.send(buffer, recipient, reliable);
    }

    @Override
    public CompletableFuture<Duration> spread(ClusterMessage message) {
        log.trace("Spreading message: message={}", message);
        metricManager.gossipGenCounters.get(message.getClusterMessageTypeCase()).increment();
        return gossiper.generateGossip(message.toByteString());
    }

    @Override
    public Observable<Timed<MessageEnvelope>> receive() {
        return publisher;
    }

    @Override
    public void start(IRecipientSelector recipientSelector) {
        switch (state) {
            case INIT:
                state = State.START;
                log.debug("Start messenger");
                disposables.add(transport.receive()
                    .observeOn(scheduler)
                    .subscribe(this::onMessengerMessage, this::onError));
                disposables.add(gossiper.gossips().observeOn(scheduler).subscribe(this::onGossipHeard));
                disposables.add(Observable
                    .interval(opts.spreadPeriod().toMillis(), opts.spreadPeriod().toMillis(), TimeUnit.MILLISECONDS)
                    .observeOn(scheduler)
                    .subscribe((tick) -> this.doGossipSpread(recipientSelector)));
                break;
            case START:
                break;
            case STOP:
                throw new IllegalStateException("Messenger has been stopped");
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        switch (state) {
            case START:
                state = State.STOP;
                log.debug("Shutdown messenger");
                metricManager.close();
                // complete message publisher
                publisher.onComplete();
                disposables.dispose();
                return transport.shutdown();
            case INIT:
                metricManager.close();
                return CompletableFuture.failedFuture(new IllegalStateException("Messenger has not started"));
            case STOP:
            default:
                return CompletableFuture.completedFuture(null);
        }
    }

    private void doGossipSpread(IRecipientSelector recipientSelector) {
        int totalGossipers = recipientSelector.clusterSize();
        int gossipPerRecipient = Math.max(1, opts.maxFanoutGossips() / opts.maxFanout());
        long period = gossiper.nextPeriod(totalGossipers);
        recipientSelector.selectForSpread(opts.maxFanout()).forEach(recipient -> {
            List<GossipMessage> gossips = gossiper.selectGossipsSendTo(recipient.addr(), totalGossipers);
            if (!gossips.isEmpty()) {
                gossips = RandomUtils.uniqueRandomPickAtMost(gossips, gossipPerRecipient, gossipMessage -> true);
                log.trace("Gossiping[{}], send gossips: msg-count={}, addr={}", period, gossips.size(),
                    recipient.addr());
                metricManager.gossipSpreadCounter.increment(gossips.size());
                transport.send(gossips.stream()
                    .map(gossipMessage -> MessengerMessage
                        .newBuilder()
                        .setGossip(gossipMessage)
                        .build())
                    .collect(Collectors.toList()), recipient.addr(), false);
            }
        });
    }

    private void onMessengerMessage(Timed<MessengerMessageEnvelope> timedMessageEnvelop) {
        MessengerMessageEnvelope messengerMessageEnvelope = timedMessageEnvelop.value();
        switch (messengerMessageEnvelope.message.getMessengerMessageTypeCase()) {
            case DIRECT:
                try {
                    ClusterMessage clusterMessage =
                        ClusterMessage.parseFrom(messengerMessageEnvelope.message.getDirect().getPayload());
                    log.trace("Received message: sender={}, message={}",
                        messengerMessageEnvelope.sender, clusterMessage);
                    metricManager.msgRecvCounters.get(clusterMessage.getClusterMessageTypeCase()).increment();
                    publisher.onNext(new Timed<>(MessageEnvelope.builder()
                        .message(clusterMessage)
                        .recipient(messengerMessageEnvelope.recipient)
                        .sender(messengerMessageEnvelope.sender)
                        .build(),
                        timedMessageEnvelop.time(),
                        timedMessageEnvelop.unit()));
                } catch (InvalidProtocolBufferException e) {
                    log.error("Invalid message", e);
                }
                break;
            case GOSSIP:
                gossiper.hearGossip(messengerMessageEnvelope.message.getGossip(), messengerMessageEnvelope.sender);
                break;
        }
    }

    private void onGossipHeard(Timed<GossipMessage> confirmedGossip) {
        try {
            ClusterMessage clusterMessage = ClusterMessage.parseFrom(confirmedGossip.value().getPayload());
            log.trace("Heard gossip: id={}, message={}", confirmedGossip.value().getMessageId(), clusterMessage);
            metricManager.gossipHeardCounters.get(clusterMessage.getClusterMessageTypeCase()).increment();
            publisher.onNext(new Timed<>(MessageEnvelope.builder()
                .message(clusterMessage)
                .recipient(localAddress)
                .build(),
                confirmedGossip.time(),
                confirmedGossip.unit()));
        } catch (InvalidProtocolBufferException e) {
            log.error("Invalid message", e);
        }
    }

    private void onError(Throwable throwable) {
        log.error("Received unexpected error:", throwable);
    }

    private static class MetricManager {
        final Map<ClusterMessage.ClusterMessageTypeCase, Counter> msgSendCounters = Maps.newHashMap();
        final Map<ClusterMessage.ClusterMessageTypeCase, Counter> msgRecvCounters = Maps.newHashMap();
        final Map<ClusterMessage.ClusterMessageTypeCase, Counter> gossipGenCounters = Maps.newHashMap();
        final Map<ClusterMessage.ClusterMessageTypeCase, Counter> gossipHeardCounters = Maps.newHashMap();
        final Counter gossipSpreadCounter = Metrics.counter("cluster.gossip.count");

        MetricManager(InetSocketAddress localAddress) {
            for (ClusterMessage.ClusterMessageTypeCase typeCase : ClusterMessage.ClusterMessageTypeCase.values()) {
                Tags tags = Tags
                    .of("local", localAddress.getAddress().getHostAddress() + ":" + localAddress.getPort())
                    .and("type", typeCase.name());
                msgSendCounters.put(typeCase,
                    Metrics.counter("basecluster.send.count", tags));
                msgRecvCounters.put(typeCase,
                    Metrics.counter("basecluster.recv.count", tags));
                gossipGenCounters.put(typeCase,
                    Metrics.counter("basecluster.gossip.gen.count", tags));
                gossipHeardCounters.put(typeCase,
                    Metrics.counter("basecluster.gossip.heard.count", tags));
            }
        }

        void close() {
            msgSendCounters.forEach((t, m) -> Metrics.globalRegistry.remove(m));
            msgRecvCounters.forEach((t, m) -> Metrics.globalRegistry.remove(m));
            gossipGenCounters.forEach((t, m) -> Metrics.globalRegistry.remove(m));
            gossipHeardCounters.forEach((t, m) -> Metrics.globalRegistry.remove(m));
            Metrics.globalRegistry.remove(gossipSpreadCounter);
        }
    }
}
