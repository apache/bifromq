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

package org.apache.bifromq.basecluster.messenger;

import org.apache.bifromq.basecluster.messenger.proto.GossipMessage;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class Gossiper {

    @Builder
    private static class GossipState {
        public final GossipMessage message;

        public final long infectionPeriod;

        private final CompletableFuture<Duration> spreadSuccessSignal = new CompletableFuture<>();

        private final long start = System.nanoTime();

        private final Set<InetSocketAddress> infected = new HashSet<>();

        @Getter
        public boolean confirmed;

        void addInfectedAddress(InetSocketAddress address) {
            infected.add(address);
        }

        boolean isInfected(InetSocketAddress address) {
            return infected.contains(address);
        }

        CompletableFuture<Duration> spreadSuccessSignal() {
            return spreadSuccessSignal;
        }

        void confirmSpreadSuccess() {
            spreadSuccessSignal.complete(Duration.ofNanos(System.nanoTime() - start));
            confirmed = true;
        }
    }

    private long currentPeriod = 0;
    private long gossipCounter = 0;
    // nanos
    private long prevPeriodTime = -1;

    private final Cache<String, GossipState> currentGossips;

    private final String id;

    private final int retransmitMultiplier;

    private final Duration spreadPeriod;

    private final Subject<GossipMessage> gossipPublisher;

    private final Observable<Timed<GossipMessage>> gossipSink;

    Gossiper(String id, int retransmitMultiplier, Duration spreadPeriod, Scheduler scheduler) {
        // global unique id of the gossiper
        this.id = id;
        this.retransmitMultiplier = retransmitMultiplier;
        this.spreadPeriod = spreadPeriod;
        gossipPublisher = PublishSubject.<GossipMessage>create().toSerialized();
        gossipSink = gossipPublisher.timestamp(scheduler);
        this.currentGossips = Caffeine.newBuilder()
            .maximumSize(1_000_000L)
            .build();
    }

    public CompletableFuture<Duration> generateGossip(ByteString payload) {
        GossipMessage gossipMessage = GossipMessage.newBuilder()
            .setMessageId(id + "-" + gossipCounter++)
            .setPayload(payload)
            .build();
        GossipState state = GossipState.builder()
            .message(gossipMessage)
            .infectionPeriod(currentPeriod)
            .build();
        currentGossips.put(gossipMessage.getMessageId(), state);
        // notify self
        gossipPublisher.onNext(gossipMessage);
        return state.spreadSuccessSignal();
    }

    public void hearGossip(GossipMessage gossipMessage, InetSocketAddress from) {
        GossipState state = currentGossips.asMap().computeIfAbsent(gossipMessage.getMessageId(), (id) -> {
            gossipPublisher.onNext(gossipMessage);
            return GossipState.builder().message(gossipMessage).infectionPeriod(currentPeriod).build();
        });
        state.addInfectedAddress(from);
    }

    public long nextPeriod(int totalGossipers) {
        long currentPeriodTime = System.nanoTime();
        currentPeriod++;
        int periodsToSpread = gossipPeriodsToSpread(retransmitMultiplier, totalGossipers);
        int periodsToSweep = gossipPeriodsToSweep(retransmitMultiplier, totalGossipers);
        if (prevPeriodTime != -1) {
            long elapsedPeriod = (currentPeriodTime - prevPeriodTime) / spreadPeriod.toNanos();
            prevPeriodTime = currentPeriodTime;
            // If time interval between current and previous period is too long, do confirm or sweep in advance
            if (elapsedPeriod > periodsToSweep) {
                log.warn("Too many elapsed periods, sweep gossips in advance, currentPeriod={}, elapsedPeriod={}",
                    currentPeriod, elapsedPeriod);
                sweepSpreadGossips(gossipState -> true);
                return currentPeriod;
            }
            if (elapsedPeriod > periodsToSpread) {
                log.warn("Some gossips are too old to spread, confirm gossips in advance, "
                        + "currentPeriod={}, elapsedPeriod={}",
                    currentPeriod, elapsedPeriod);
                double confirmRatio = elapsedPeriod / (double) periodsToSweep;
                confirmGossipsSpread(gossipState ->
                    !gossipState.confirmed && (
                        ThreadLocalRandom.current().nextDouble() < confirmRatio ||
                            currentPeriod > gossipState.infectionPeriod + periodsToSpread)
                );
                return currentPeriod;
            }
        }
        // do normal period action
        sweepSpreadGossips(gossipState -> currentPeriod > gossipState.infectionPeriod + periodsToSweep);
        confirmGossipsSpread(gossipState -> !gossipState.confirmed &&
            currentPeriod > gossipState.infectionPeriod + periodsToSpread);
        prevPeriodTime = currentPeriodTime;
        return currentPeriod;
    }

    public List<GossipMessage> selectGossipsSendTo(InetSocketAddress remoteAddress, int totalGossipers) {
        int periodsToSpread = gossipPeriodsToSpread(retransmitMultiplier, totalGossipers);
        return currentGossips.asMap().values().stream()
            .filter(gossipState -> !gossipState.confirmed &&
                gossipState.infectionPeriod + periodsToSpread >= currentPeriod)
            .filter(gossipState -> !gossipState.isInfected(remoteAddress)) // filter already infected
            .map(gossipState -> gossipState.message)
            .collect(Collectors.toList());
    }

    public Observable<Timed<GossipMessage>> gossips() {
        return gossipSink;
    }

    private void sweepSpreadGossips(Predicate<GossipState> predicate) {
        currentGossips.asMap().entrySet().removeIf(e -> {
            GossipState state = e.getValue();
            if (predicate.test(state)) {
                log.trace("Sweep gossip: messageId={}, currentPeriod={}, infectedPeriod={}",
                    e.getKey(), currentPeriod, state.infectionPeriod);
                return true;
            }
            return false;
        });
    }

    private void confirmGossipsSpread(Predicate<GossipState> predicate) {
        currentGossips.asMap().values().stream()
            .filter(predicate)
            .forEach(gossipState -> {
                log.trace("Confirm gossip: messageId={}, currentPeriod={}, infectedPeriod={}",
                    gossipState.message.getMessageId(), currentPeriod, gossipState.infectionPeriod);
                gossipState.confirmSpreadSuccess();
            });
    }

    @VisibleForTesting
    int gossipPeriodsToSpread(int retransmitMultiplier, int totalGossipers) {
        return retransmitMultiplier * ceilLog2(totalGossipers);
    }

    @VisibleForTesting
    int gossipPeriodsToSweep(int retransmitMultiplier, int totalGossipers) {
        int periodsToSpread = gossipPeriodsToSpread(retransmitMultiplier, totalGossipers);
        return Math.max(100, (int) Math.pow(periodsToSpread + 1, 2));
    }

    @VisibleForTesting
    int ceilLog2(int num) {
        return num <= 1 ? 1 : 32 - Integer.numberOfLeadingZeros(num - 1);
    }
}
