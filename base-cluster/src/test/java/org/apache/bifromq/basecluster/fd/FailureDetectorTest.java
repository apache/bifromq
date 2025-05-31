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

import static org.apache.bifromq.basecluster.fd.Fixtures.toPing;
import static org.apache.bifromq.basecluster.fd.Fixtures.toPingAck;
import static org.apache.bifromq.basecluster.fd.Fixtures.toPingNack;
import static org.apache.bifromq.basecluster.fd.Fixtures.toPingReq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.bifromq.basecluster.messenger.IMessenger;
import org.apache.bifromq.basecluster.messenger.MessageEnvelope;
import org.apache.bifromq.basecluster.proto.ClusterMessage;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.schedulers.TestScheduler;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class FailureDetectorTest {
    @Mock
    IMessenger messenger;
    @Mock
    IProbingTargetSelector targetSelector;
    @Mock
    Consumer<Integer> healthScoreConsumer;
    @Mock
    Consumer<Timed<IProbingTarget>> successProbeConsumer;
    @Mock
    Consumer<Timed<IProbingTarget>> suspectProbeConsumer;
    private TestScheduler scheduler;
    private PublishSubject<MessageEnvelope> messageSource;
    private FailureDetector failureDetector;
    private AutoCloseable closeable;
    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        scheduler = new TestScheduler();
        messageSource = PublishSubject.create();
        when(messenger.receive()).thenReturn(messageSource.timestamp());
        failureDetector = FailureDetector.builder()
            .local(Fixtures.LOCAL_PROBING_TARGET)
            .messenger(messenger)
            .scheduler(scheduler)
            .baseProbeInterval(Fixtures.BASE_PROBE_INTERVAL)
            .baseProbeTimeout(Fixtures.BASE_PROBE_TIMEOUT)
            .indirectProbes(Fixtures.INDIRECT_PROBES)
            .worstHealthScore(Fixtures.WORST_HEALTH_SCORE)
            .build();
        failureDetector.healthScoring().subscribe(healthScoreConsumer);
        failureDetector.succeeding().subscribe(successProbeConsumer);
        failureDetector.suspecting().subscribe(suspectProbeConsumer);
        assertEquals(failureDetector.baseProbeInterval(), Fixtures.BASE_PROBE_INTERVAL);
        assertEquals(failureDetector.baseProbeTimeout(), Fixtures.BASE_PROBE_TIMEOUT);
    }

    @AfterMethod
    public void teardown() throws Exception {
        closeable.close();
    }

    @Test
    public void shutdownBeforeStart() {
        try {
            failureDetector.shutdown().join();
            fail();
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void sendDirectProbe() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        failureDetector.start(targetSelector);
        // wait for probe start
        scheduler.advanceTimeTo(1000, TimeUnit.MILLISECONDS);

        ArgumentCaptor<ClusterMessage> msgCaptor = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<List<ClusterMessage>> piggybackMsgsCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<InetSocketAddress> targetAddrCaptor = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger).send(msgCaptor.capture(),
            piggybackMsgsCaptor.capture(),
            targetAddrCaptor.capture(),
            reliableCaptor.capture());

        assertEquals(msgCaptor.getValue(), toPing(0, Fixtures.LOCAL_PROBING_TARGET, Fixtures.DIRECT_PROBING_TARGET));
        assertTrue(piggybackMsgsCaptor.getValue().isEmpty());
        assertEquals(targetAddrCaptor.getValue(), Fixtures.DIRECT_TARGET_ADDRESS);
        assertFalse(reliableCaptor.getValue());
    }

    @SneakyThrows
    @Test
    public void receivePingAckDuringDirectProbe() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        failureDetector.start(targetSelector);
        // wait for probe start
        scheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);

        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingAck(0))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.DIRECT_TARGET_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        verify(healthScoreConsumer, times(2)).accept(0);
        ArgumentCaptor<Timed<IProbingTarget>> targetCap = ArgumentCaptor.forClass(Timed.class);
        verify(successProbeConsumer).accept(targetCap.capture());
        Timed<IProbingTarget> successProbe = targetCap.getValue();
        assertTrue(successProbe.time() > 0);
        assertEquals(successProbe.value(), Fixtures.DIRECT_PROBING_TARGET);
    }

    @SneakyThrows
    @Test
    public void receiveDupPingAckDuringDirectProbe() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        failureDetector.start(targetSelector);
        // wait for probe start
        scheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);

        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingAck(0))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.DIRECT_TARGET_ADDRESS)
            .build());
        // duplicate ping ack
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingAck(0))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.DIRECT_TARGET_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        verify(healthScoreConsumer, times(2)).accept(0);
        ArgumentCaptor<Timed<IProbingTarget>> targetCap = ArgumentCaptor.forClass(Timed.class);
        verify(successProbeConsumer).accept(targetCap.capture());
        Timed<IProbingTarget> successProbe = targetCap.getValue();
        assertTrue(successProbe.time() > 0);
        assertEquals(successProbe.value(), Fixtures.DIRECT_PROBING_TARGET);
    }

    @SneakyThrows
    @Test
    public void scheduleNextProbeAfterDirectProbeSuccess() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        failureDetector.start(targetSelector);
        // wait for probe start
        scheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);

        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingAck(0))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.DIRECT_TARGET_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        // wait for next probe start
        scheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingAck(1))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.DIRECT_TARGET_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        verify(healthScoreConsumer, times(3)).accept(0);
        verify(successProbeConsumer, times(2)).accept(any());
    }

    @SneakyThrows
    @Test
    public void directProbeTimeoutAndSuspect() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        // no indirect probers
        when(targetSelector.targetForIndirectProbes(any(), anyInt())).thenReturn(Collections.emptyList());

        failureDetector.start(targetSelector);

        // wait for probe start and wait for timeout
        scheduler.advanceTimeBy(1500, TimeUnit.MILLISECONDS);

        ArgumentCaptor<Timed<IProbingTarget>> targetCap = ArgumentCaptor.forClass(Timed.class);
        verify(suspectProbeConsumer).accept(targetCap.capture());
        Timed<IProbingTarget> suspectProbe = targetCap.getValue();
        assertTrue(suspectProbe.time() > 0);
        assertEquals(suspectProbe.value(), Fixtures.DIRECT_PROBING_TARGET);
    }

    @SneakyThrows
    @Test
    public void scheduleNextProbeAfterTimeout() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        failureDetector.start(targetSelector);
        // wait for probe start
        scheduler.advanceTimeBy(3000, TimeUnit.MILLISECONDS);

        verify(messenger, times(2)).send(any(), anyList(), any(), anyBoolean());
        verify(suspectProbeConsumer, times(2)).accept(any());
    }

    @Test
    public void sendIndirectProbesAfterDirectProbeTimeout() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        when(targetSelector.targetForIndirectProbes(Fixtures.DIRECT_PROBING_TARGET, Fixtures.INDIRECT_PROBES))
            .thenReturn(Arrays.asList(Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.INDIRECT_PROBING_TARGET_2));
        failureDetector.start(targetSelector);
        // wait for probe start
        scheduler.advanceTimeBy(1500, TimeUnit.MILLISECONDS);

        ArgumentCaptor<ClusterMessage> msgCaptor = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<List<ClusterMessage>> piggybackMsgsCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<InetSocketAddress> targetAddrCaptor = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger, times(3)).send(msgCaptor.capture(),
            piggybackMsgsCaptor.capture(),
            targetAddrCaptor.capture(),
            reliableCaptor.capture());

        assertEquals(msgCaptor.getAllValues().get(1),
            toPingReq(0, Fixtures.LOCAL_PROBING_TARGET, Fixtures.DIRECT_PROBING_TARGET));
        assertEquals(msgCaptor.getAllValues().get(2),
            toPingReq(0, Fixtures.LOCAL_PROBING_TARGET, Fixtures.DIRECT_PROBING_TARGET));

        assertTrue(piggybackMsgsCaptor.getAllValues().get(1).isEmpty());
        assertTrue(piggybackMsgsCaptor.getAllValues().get(2).isEmpty());

        assertEquals(targetAddrCaptor.getAllValues().get(1), Fixtures.INDIRECT_TARGET_ADDRESS_1);
        assertEquals(targetAddrCaptor.getAllValues().get(2), Fixtures.INDIRECT_TARGET_ADDRESS_2);

        assertFalse(reliableCaptor.getAllValues().get(1));
        assertFalse(reliableCaptor.getAllValues().get(2));
    }

    @SneakyThrows
    @Test
    public void indirectProbesTimeout() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        when(targetSelector.targetForIndirectProbes(Fixtures.DIRECT_PROBING_TARGET, Fixtures.INDIRECT_PROBES))
            .thenReturn(Arrays.asList(Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.INDIRECT_PROBING_TARGET_2));
        failureDetector.start(targetSelector);
        // wait for probe start
        scheduler.advanceTimeBy(3500, TimeUnit.MILLISECONDS);

        verify(healthScoreConsumer).accept(3);

        ArgumentCaptor<Timed<IProbingTarget>> targetCap = ArgumentCaptor.forClass(Timed.class);
        verify(suspectProbeConsumer).accept(targetCap.capture());
        Timed<IProbingTarget> suspectProbe = targetCap.getValue();
        assertTrue(suspectProbe.time() > 0);
        assertEquals(suspectProbe.value(), Fixtures.DIRECT_PROBING_TARGET);
    }

    @SneakyThrows
    @Test
    public void scheduleNextProbeAfterIndirectProbesTimeout() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        when(targetSelector.targetForIndirectProbes(Fixtures.DIRECT_PROBING_TARGET, Fixtures.INDIRECT_PROBES))
            .thenReturn(Arrays.asList(Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.INDIRECT_PROBING_TARGET_2));
        failureDetector.start(targetSelector);
        // wait for probe start
        scheduler.advanceTimeBy(3000, TimeUnit.MILLISECONDS);

        verify(messenger, times(4)).send(any(), anyList(), any(), anyBoolean());
    }

    @SneakyThrows
    @Test
    public void receiveSomeNackAndTimeout() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        when(targetSelector.targetForIndirectProbes(Fixtures.DIRECT_PROBING_TARGET, Fixtures.INDIRECT_PROBES))
            .thenReturn(Arrays.asList(Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.INDIRECT_PROBING_TARGET_2));
        failureDetector.start(targetSelector);
        scheduler.advanceTimeBy(1500, TimeUnit.MILLISECONDS);

        // ping nack to ping req
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingNack(0))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.DIRECT_TARGET_ADDRESS)
            .build());
        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);
        verify(healthScoreConsumer).accept(2);

        ArgumentCaptor<Timed<IProbingTarget>> targetCap = ArgumentCaptor.forClass(Timed.class);
        verify(suspectProbeConsumer).accept(targetCap.capture());
        Timed<IProbingTarget> suspectProbe = targetCap.getValue();
        assertTrue(suspectProbe.time() > 0);
        assertEquals(suspectProbe.value(), Fixtures.DIRECT_PROBING_TARGET);
    }

    @SneakyThrows
    @Test
    public void indirectProbeSuccess() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        when(targetSelector.targetForIndirectProbes(Fixtures.DIRECT_PROBING_TARGET, Fixtures.INDIRECT_PROBES))
            .thenReturn(Arrays.asList(Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.INDIRECT_PROBING_TARGET_2));
        failureDetector.start(targetSelector);
        scheduler.advanceTimeBy(1500, TimeUnit.MILLISECONDS);

        // ping ack to ping req
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingAck(0))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.DIRECT_TARGET_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        verify(healthScoreConsumer, times(2)).accept(0);

        ArgumentCaptor<Timed<IProbingTarget>> targetCap = ArgumentCaptor.forClass(Timed.class);
        verify(successProbeConsumer).accept(targetCap.capture());
        Timed<IProbingTarget> suspectProbe = targetCap.getValue();
        assertTrue(suspectProbe.time() > 0);
        assertEquals(suspectProbe.value(), Fixtures.DIRECT_PROBING_TARGET);
    }

    @Test
    public void scheduleNextProbeAfterIndirectProbeSuccess() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        when(targetSelector.targetForIndirectProbes(Fixtures.DIRECT_PROBING_TARGET, Fixtures.INDIRECT_PROBES))
            .thenReturn(Arrays.asList(Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.INDIRECT_PROBING_TARGET_2));
        failureDetector.start(targetSelector);
        scheduler.advanceTimeBy(1500, TimeUnit.MILLISECONDS);

        // ping ack to ping req
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingAck(0))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.DIRECT_TARGET_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);

        verify(messenger, times(4)).send(any(), anyList(), any(), anyBoolean());
    }

    @SneakyThrows
    @Test
    public void receiveAllNack() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        when(targetSelector.targetForIndirectProbes(Fixtures.DIRECT_PROBING_TARGET, Fixtures.INDIRECT_PROBES))
            .thenReturn(Arrays.asList(Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.INDIRECT_PROBING_TARGET_2));
        failureDetector.start(targetSelector);
        scheduler.advanceTimeBy(1500, TimeUnit.MILLISECONDS);

        // ping nack to ping req
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingNack(0))
            .recipient(Fixtures.INDIRECT_TARGET_ADDRESS_1)
            .sender(Fixtures.LOCAL_ADDRESS)
            .build());
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingNack(0))
            .recipient(Fixtures.INDIRECT_TARGET_ADDRESS_2)
            .sender(Fixtures.LOCAL_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        verify(healthScoreConsumer, times(2)).accept(0);

        ArgumentCaptor<Timed<IProbingTarget>> targetCap = ArgumentCaptor.forClass(Timed.class);
        verify(suspectProbeConsumer).accept(targetCap.capture());
        Timed<IProbingTarget> suspectProbe = targetCap.getValue();
        assertTrue(suspectProbe.time() > 0);
        assertEquals(suspectProbe.value(), Fixtures.DIRECT_PROBING_TARGET);
    }

    @Test
    public void scheduleNextProbeAfterReceivedAllNack() {
        DirectProbingInfo probingInfo = new DirectProbingInfo(Optional.of(Fixtures.DIRECT_PROBING_TARGET));
        when(targetSelector.targetForProbe()).thenReturn(probingInfo);
        when(targetSelector.targetForIndirectProbes(Fixtures.DIRECT_PROBING_TARGET, Fixtures.INDIRECT_PROBES))
            .thenReturn(Arrays.asList(Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.INDIRECT_PROBING_TARGET_2));
        failureDetector.start(targetSelector);
        scheduler.advanceTimeBy(1500, TimeUnit.MILLISECONDS);

        // ping nack to ping req
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingNack(0))
            .recipient(Fixtures.INDIRECT_TARGET_ADDRESS_1)
            .sender(Fixtures.LOCAL_ADDRESS)
            .build());
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingNack(0))
            .recipient(Fixtures.INDIRECT_TARGET_ADDRESS_2)
            .sender(Fixtures.LOCAL_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
        verify(messenger, times(4)).send(any(), anyList(), any(), anyBoolean());
    }

    @Test
    public void handlePingAndAck() {
        failureDetector.start(targetSelector);
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPing(0, Fixtures.DIRECT_PROBING_TARGET, Fixtures.LOCAL_PROBING_TARGET))
            .recipient(Fixtures.DIRECT_TARGET_ADDRESS)
            .sender(Fixtures.LOCAL_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);


        ArgumentCaptor<ClusterMessage> msgCaptor = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> targetAddrCaptor = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger).send(msgCaptor.capture(), targetAddrCaptor.capture(), reliableCaptor.capture());

        assertEquals(msgCaptor.getValue(), toPingAck(0));
        assertEquals(targetAddrCaptor.getValue(), Fixtures.DIRECT_TARGET_ADDRESS);
        assertFalse(reliableCaptor.getValue());
    }

    @Test
    public void handlePingAndIgnore() {
        failureDetector.start(targetSelector);
        messageSource.onNext(MessageEnvelope.builder()
            // Wrong target
            .message(toPing(0, Fixtures.DIRECT_PROBING_TARGET, Fixtures.INDIRECT_PROBING_TARGET_1))
            .recipient(Fixtures.DIRECT_TARGET_ADDRESS)
            .sender(Fixtures.LOCAL_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        verify(messenger, times(0)).send(any(ClusterMessage.class), any(InetSocketAddress.class), anyBoolean());
    }

    @Test
    public void handlePingReqAndSendPing() {
        failureDetector.start(targetSelector);
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingReq(0, Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.DIRECT_PROBING_TARGET))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.INDIRECT_TARGET_ADDRESS_1)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addrCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);
        verify(messenger).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getValue(), toPing(0, Fixtures.LOCAL_PROBING_TARGET, Fixtures.DIRECT_PROBING_TARGET));
        assertEquals(addrCap.getValue(), Fixtures.DIRECT_TARGET_ADDRESS);
        assertTrue(reliableCap.getValue());
    }

    @Test
    public void handlePingReqAndSendAck() {
        failureDetector.start(targetSelector);
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingReq(0, Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.DIRECT_PROBING_TARGET))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.INDIRECT_TARGET_ADDRESS_1)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        // got ack
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingAck(0))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.DIRECT_TARGET_ADDRESS)
            .build());
        scheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addrCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);

        verify(messenger, times(2)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getAllValues().get(1), toPingAck(0));
        assertEquals(addrCap.getAllValues().get(1), Fixtures.INDIRECT_TARGET_ADDRESS_1);
        assertTrue(reliableCap.getAllValues().get(1));
    }

    @Test
    public void handPingReqAndSendNack() {
        failureDetector.start(targetSelector);
        messageSource.onNext(MessageEnvelope.builder()
            .message(toPingReq(0, Fixtures.INDIRECT_PROBING_TARGET_1, Fixtures.DIRECT_PROBING_TARGET))
            .recipient(Fixtures.LOCAL_ADDRESS)
            .sender(Fixtures.INDIRECT_TARGET_ADDRESS_1)
            .build());
        scheduler.advanceTimeBy(500, TimeUnit.MILLISECONDS);

        ArgumentCaptor<ClusterMessage> msgCap = ArgumentCaptor.forClass(ClusterMessage.class);
        ArgumentCaptor<InetSocketAddress> addrCap = ArgumentCaptor.forClass(InetSocketAddress.class);
        ArgumentCaptor<Boolean> reliableCap = ArgumentCaptor.forClass(Boolean.class);

        verify(messenger, times(2)).send(msgCap.capture(), addrCap.capture(), reliableCap.capture());

        assertEquals(msgCap.getAllValues().get(1), toPingNack(0));
        assertEquals(addrCap.getAllValues().get(1), Fixtures.INDIRECT_TARGET_ADDRESS_1);
        assertTrue(reliableCap.getAllValues().get(1));
    }
}
