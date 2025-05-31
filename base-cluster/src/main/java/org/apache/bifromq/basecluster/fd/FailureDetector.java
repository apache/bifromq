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

package org.apache.bifromq.basecluster.fd;

import org.apache.bifromq.basecluster.fd.proto.Ack;
import org.apache.bifromq.basecluster.fd.proto.Nack;
import org.apache.bifromq.basecluster.fd.proto.Ping;
import org.apache.bifromq.basecluster.fd.proto.PingReq;
import org.apache.bifromq.basecluster.messenger.IMessenger;
import org.apache.bifromq.basecluster.messenger.MessageEnvelope;
import org.apache.bifromq.basecluster.proto.ClusterMessage;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FailureDetector implements IFailureDetector {
    private enum State {
        INIT, STARTING, STARTED, STOPPING, STOPPED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final AtomicInteger seqNo = new AtomicInteger();
    private final IMessenger messenger;
    private final Subject<IProbingTarget> normalProbeSubject;
    private final Subject<IProbingTarget> suspicionSubject;
    private final Observable<Timed<IProbingTarget>> suspicionSink;
    private final Observable<Timed<IProbingTarget>> normalProbeSink;
    private final Subject<Integer> healthScoreSubject = BehaviorSubject.createDefault(0).toSerialized();
    private final int indirectProbes;
    private final int worstHealthScore;
    private final Duration baseProbeInterval;
    private final Duration baseProbeTimeout;
    private final Scheduler scheduler;
    private final IProbingTarget local;
    private final AtomicBoolean probing = new AtomicBoolean();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private volatile Disposable probeTask;
    private IProbingTargetSelector targetSelector;

    @Builder
    private FailureDetector(IProbingTarget local,
                            IMessenger messenger,
                            Scheduler scheduler,
                            Duration baseProbeInterval,
                            Duration baseProbeTimeout,
                            int indirectProbes,
                            int worstHealthScore) {
        Preconditions.checkArgument(indirectProbes > 0, "Indirect probes must be positive");
        Preconditions.checkArgument(worstHealthScore > 0, "Worst health score must be positive");
        this.local = local;
        this.messenger = messenger;
        this.scheduler = scheduler;
        this.baseProbeInterval = baseProbeInterval;
        this.baseProbeTimeout = baseProbeTimeout;
        this.indirectProbes = indirectProbes;
        this.worstHealthScore = worstHealthScore;
        normalProbeSubject = PublishSubject.<IProbingTarget>create().toSerialized();
        normalProbeSink = normalProbeSubject.timestamp(scheduler);
        suspicionSubject = PublishSubject.<IProbingTarget>create().toSerialized();
        suspicionSink = suspicionSubject.timestamp(scheduler);
    }

    @Override
    public Duration baseProbeInterval() {
        return baseProbeInterval;
    }

    @Override
    public Duration baseProbeTimeout() {
        return baseProbeTimeout;
    }

    @Override
    public void start(IProbingTargetSelector targetSelector) {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            log.debug("Start failure detector from local[{}]@{}", local.id(), local.addr());
            this.targetSelector = targetSelector;
            disposables.add(messenger.receive().observeOn(scheduler).subscribe(this::handleMessageEnvelope));
            state.set(State.STARTED);
            scheduleProbe();
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            log.debug("Stopping failure detector");
            if (probeTask != null && !probeTask.isDisposed()) {
                probeTask.dispose();
            }
            disposables.dispose();
            // complete suspicion subject
            suspicionSubject.onComplete();
            // complete lhs subject
            healthScoreSubject.onComplete();
            state.set(State.STOPPED);
            return CompletableFuture.completedFuture(null);
        } else {
            switch (state.get()) {
                case STOPPED:
                    return CompletableFuture.completedFuture(null);
                default:
                    return CompletableFuture.failedFuture(new IllegalStateException("Failure detector not stoppable"));
            }
        }
    }

    @Override
    public Observable<Timed<IProbingTarget>> succeeding() {
        return normalProbeSink;
    }

    @Override
    public Observable<Timed<IProbingTarget>> suspecting() {
        return suspicionSink;
    }

    @Override
    public void penaltyHealth() {
        updateLocalHealthScore(healthScoreSubject.blockingFirst() + 1);
    }

    @Override
    public Observable<Integer> healthScoring() {
        return healthScoreSubject;
    }

    private void handleMessageEnvelope(Timed<MessageEnvelope> messageEnvelopeTimed) {
        if (state.get() != State.STARTED) {
            return;
        }
        ClusterMessage clusterMessage = messageEnvelopeTimed.value().message;
        switch (clusterMessage.getClusterMessageTypeCase()) {
            case PING:
                handlePing(clusterMessage.getPing());
                break;
            case PINGREQ:
                handlePingReq(clusterMessage.getPingReq());
                break;
        }
    }

    private void scheduleProbe() {
        if (state.get() == State.STARTED && probing.compareAndSet(false, true)) {
            probeTask = scheduler.scheduleDirect(() -> probe().whenComplete((v, e) -> {
                probing.set(false);
                scheduleProbe();
            }), baseProbeInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private CompletableFuture<Void> probe() {
        if (state.get() != State.STARTED) {
            return CompletableFuture.completedFuture(null);
        }
        DirectProbingInfo directProbingInfo = targetSelector.targetForProbe();
        if (!directProbingInfo.target.isPresent()) {
            log.trace("No target eligible for probing");
            return CompletableFuture.completedFuture(null);
        }
        int seqNum = nextSeqNum();
        IProbingTarget target = directProbingInfo.target.get();
        CompletableFuture<Void> doneSignal = new CompletableFuture<>();

        int score = healthScoreSubject.blockingFirst();
        Duration probeInterval = FailureDetectorMath.scale(baseProbeInterval, score);
        Duration directProbeTimeout = FailureDetectorMath.scale(baseProbeTimeout, score);
        Duration indirectProbeTimeout = probeInterval.minus(directProbeTimeout);
        log.trace("Probe with directTimeout[{}] and indirectTimeout[{}]: target={}, addr={}, localHealthScore={}",
            directProbeTimeout.toMillis(), indirectProbeTimeout.toMillis(), target.id(), target.addr(), score);
        // setup a ack observer during direct probe period
        messenger.receive()
            .filter(msg -> {
                ClusterMessage clusterMsg = msg.value().message;
                return clusterMsg.hasAck() && clusterMsg.getAck().getSeqNo() == seqNum;
            })
            .map(msg -> msg.value().message.getAck())
            .take(1) // only take first one and complete
            .timeout(directProbeTimeout.toMillis(), TimeUnit.MILLISECONDS, scheduler) // or raise TimeoutException
            .observeOn(scheduler)
            .subscribe(ack -> {
                if (state.get() != State.STARTED) {
                    doneSignal.complete(null);
                    return;
                }
                // received ack within direct probe timeout
                log.trace("Probe[{}], direct probing succeed with ack: target={}, addr={}",
                    seqNum, target.id(), target.addr());
                // increase local health score by -1
                updateLocalHealthScore(score - 1);
                normalProbeSubject.onNext(target);
                doneSignal.complete(null);
            }, e -> {
                assert e instanceof TimeoutException;
                if (state.get() != State.STARTED) {
                    doneSignal.complete(null);
                    return;
                }
                log.trace("Probe[{}], direct probing timeout: target={}, addr={}",
                    seqNum, target.id(), target.addr());
                // no ack received during direct probing phase
                // adjust local health delta by 1
                AtomicInteger localHealthDelta = new AtomicInteger(1);
                // prepare indirect probeTargets
                Collection<? extends IProbingTarget> indirectProbers = targetSelector
                    .targetForIndirectProbes(target, indirectProbes);
                if (!indirectProbers.isEmpty()) {
                    ClusterMessage pingReq = ClusterMessage.newBuilder()
                        .setPingReq(PingReq.newBuilder()
                            .setSeqNo(seqNum)
                            .setId(target.id())
                            .setAddr(target.addr().getAddress().getHostAddress())
                            .setPort(target.addr().getPort())
                            .setPingerId(local.id())
                            .setPingerAddr(local.addr().getAddress().getHostAddress())
                            .setPingerPort(local.addr().getPort())
                            .build())
                        .build();
                    final int expectedNacks = indirectProbers.size();
                    // adjust local health delta by expectedNacks
                    localHealthDelta.addAndGet(expectedNacks);
                    AtomicInteger nackReceived = new AtomicInteger(0);
                    // setup a ack and nack observer during indirect probe phase
                    messenger.receive()
                        .map(envelop -> envelop.value().message)
                        .filter(clusterMsg ->
                            (clusterMsg.hasNack() && clusterMsg.getNack().getSeqNo() == seqNum) ||
                                (clusterMsg.hasAck() && clusterMsg.getAck().getSeqNo() == seqNum))
                        .timeout(indirectProbeTimeout.toMillis(), TimeUnit.MILLISECONDS, scheduler)
                        .observeOn(scheduler)
                        .subscribe(new Observer<>() {
                            private Disposable indirectProbeObserver;

                            @Override
                            public void onSubscribe(@NonNull Disposable d) {
                                indirectProbeObserver = d;
                            }

                            @Override
                            public void onNext(@NonNull ClusterMessage clusterMessage) {
                                if (state.get() != State.STARTED) {
                                    indirectProbeObserver.dispose();
                                    doneSignal.complete(null);
                                    return;
                                }
                                switch (clusterMessage.getClusterMessageTypeCase()) {
                                    case ACK:
                                        log.trace("Probe[{}], indirect probing succeed with ack: target={}, " +
                                                "addr={}",
                                            seqNum, target.id(), target.addr());
                                        // indirect probe succeed
                                        // adjust local health delta by -1
                                        localHealthDelta.addAndGet(-1 - (expectedNacks - nackReceived.get()));
                                        normalProbeSubject.onNext(target);
                                        updateLocalHealthScore(score + localHealthDelta.get());
                                        indirectProbeObserver.dispose();
                                        doneSignal.complete(null);
                                        break;
                                    case NACK:
                                        log.trace(
                                            "Probe[{}], indirect probing received nack: target={}, addr={}",
                                            seqNum, target.id(), target.addr());
                                        // adjust local health delta by -1
                                        localHealthDelta.addAndGet(-1);
                                        // if all expected nacks are received
                                        if (nackReceived.incrementAndGet() >= expectedNacks) {
                                            log.trace("Probe[{}], indirect probing finished with all " +
                                                "nack received, raise suspicion: " + "target={}, " +
                                                "addr={}", seqNum, target.id(), target.addr());
                                            // cancel the delta of no ack during direct probe phase
                                            localHealthDelta.addAndGet(-1);
                                            suspicionSubject.onNext(target);
                                            updateLocalHealthScore(score + localHealthDelta.get());
                                            indirectProbeObserver.dispose();
                                            doneSignal.complete(null);
                                        }
                                        break;
                                }
                            }

                            @Override
                            public void onError(@NonNull Throwable e) {
                                assert e instanceof TimeoutException;
                                if (state.get() != State.STARTED) {
                                    doneSignal.complete(null);
                                    return;
                                }
                                log.trace(
                                    "Probe[{}], indirect probing timeout, raise suspicion: target={}, " +
                                        "addr={}",
                                    seqNum, target.id(), target.addr());
                                suspicionSubject.onNext(target);
                                updateLocalHealthScore(score + localHealthDelta.get());
                                doneSignal.complete(null);
                            }

                            @Override
                            public void onComplete() {

                            }
                        });

                    log.trace("Probe[{}], indirect probing start: target={}, addr={}, count={}",
                        seqNum, target.id(), target.addr(), indirectProbers.size());
                    // send out PingReq
                    indirectProbers.forEach(indirectTarget -> {
                        log.trace("Probe[{}], sending ping-req: target={}, addr={}",
                            seqNum, indirectTarget.id(), indirectTarget.addr());
                        messenger.send(pingReq, directProbingInfo.piggybacked, indirectTarget.addr(), false);
                    });
                } else {
                    // no indirect probers available, raise suspect
                    suspicionSubject.onNext(target);
                    doneSignal.complete(null);
                }
            });
        ClusterMessage ping = ClusterMessage.newBuilder()
            .setPing(Ping.newBuilder()
                .setSeqNo(seqNum)
                .setId(target.id())
                .setPingerId(local.id())
                .setPingerAddr(local.addr().getAddress().getHostAddress())
                .setPingerPort(local.addr().getPort())
                .build())
            .build();
        // send direct ping
        log.trace("Probe[{}], direct probing start: target={}, addr={}", seqNum, target.id(), target.addr());
        messenger.send(ping, directProbingInfo.piggybacked, target.addr(), false);
        return doneSignal;
    }

    private void updateLocalHealthScore(int rawScore) {
        healthScoreSubject.onNext(Ints.constrainToRange(rawScore, 0, worstHealthScore));
    }

    private void handlePing(Ping ping) {
        // not about us
        if (!ping.getId().equals(local.id())) {
            return;
        }
        ClusterMessage ack = ClusterMessage.newBuilder()
            .setAck(Ack.newBuilder().setSeqNo(ping.getSeqNo()).build())
            .build();
        log.trace("Received ping and sending ack: ping={}, ack={}", ping, ack);
        messenger.send(ack, new InetSocketAddress(ping.getPingerAddr(), ping.getPingerPort()), false);
    }

    private void handlePingReq(PingReq pingReq) {
        int seqNum = nextSeqNum();
        messenger.receive()
            .filter(msg -> msg.value().message.hasAck() && msg.value().message.getAck().getSeqNo() == seqNum)
            .map(msg -> msg.value().message.getAck())
            .take(1)
            // setup a timer to send nack back to origin probing node if no ack received during ProbeTimeout
            .timeout(baseProbeTimeout.toMillis(), TimeUnit.MILLISECONDS, scheduler)
            .observeOn(scheduler)
            .subscribe(ack -> {
                ClusterMessage relayAck = ClusterMessage.newBuilder()
                    .setAck(Ack.newBuilder()
                        .setSeqNo(pingReq.getSeqNo())
                        .build())
                    .build();
                log.trace("Received ack and relaying back: ack={}, addr={}:{}",
                    ack, pingReq.getPingerAddr(), pingReq.getPort());
                messenger.send(relayAck, new InetSocketAddress(pingReq.getPingerAddr(), pingReq.getPort()), true);
            }, e -> {
                assert e instanceof TimeoutException;
                ClusterMessage nack = ClusterMessage.newBuilder()
                    .setNack(Nack.newBuilder()
                        .setSeqNo(pingReq.getSeqNo())
                        .build())
                    .build();
                log.trace("Send nack: nack={}, addr={}:{}", nack, pingReq.getPingerAddr(), pingReq.getPort());
                messenger.send(nack, new InetSocketAddress(pingReq.getPingerAddr(), pingReq.getPort()), true);
            });
        // send out the indirect ping
        ClusterMessage ping = ClusterMessage.newBuilder()
            .setPing(Ping.newBuilder()
                .setSeqNo(seqNum)
                .setId(pingReq.getId())
                .setPingerId(local.id())
                .setPingerAddr(local.addr().getAddress().getHostAddress())
                .setPingerPort(local.addr().getPort())
                .build())
            .build();
        log.trace("Received ping-req and sending ping: pingreq={}, ping={}", pingReq, ping);
        // indirect ping go via reliable channel
        messenger.send(ping, new InetSocketAddress(pingReq.getAddr(), pingReq.getPort()), true);
    }

    private int nextSeqNum() {
        return seqNo.getAndIncrement();
    }
}
