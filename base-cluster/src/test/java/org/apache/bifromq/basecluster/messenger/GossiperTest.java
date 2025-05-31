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

import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecluster.messenger.proto.GossipMessage;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.schedulers.Timed;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Created by mafei01 in 2020-04-24 10:24
 */
@Slf4j
public class GossiperTest {

    private Gossiper gossiper;
    private InetSocketAddress local = new InetSocketAddress("127.0.0.1", 12345);
    private InetSocketAddress remote = new InetSocketAddress("127.0.0.2", 12345);
    private InetSocketAddress remote2 = new InetSocketAddress("127.0.0.3", 12345);

    private String msgId1 = "127.0.0.1:8888-1";
    private String msgId2 = "127.0.0.1:8888-2";
    private String msgPayload1 = "remote1";
    private String msgPayload2 = "remote2";
    private int retransmitMultiplier = 1;
    private Duration period = Duration.ofMillis(10);

    @BeforeMethod
    public void init() {
        gossiper = new Gossiper(local.toString(), retransmitMultiplier, period, Schedulers.single());
    }

    @Test
    public void testGenerateGossip() {
        TestSubscriber<GossipMessage> gossips = TestSubscriber.create();
        gossiper.gossips()
            .subscribe(g -> gossips.onNext(g.value()));
        gossiper.generateGossip(ByteString.copyFromUtf8("test"));
        gossips.assertValueCount(1);
        assertTrue(gossips.values().get(0).getMessageId().startsWith(local.toString()));
    }

    @Test
    public void testHearGossipWithSameId() {
        TestSubscriber<GossipMessage> gossips = TestSubscriber.create();
        gossiper.gossips()
            .subscribe(g -> gossips.onNext(g.value()));
        gossiper.hearGossip(GossipMessage.newBuilder()
            .setMessageId(msgId1).setPayload(ByteString.copyFromUtf8(msgPayload1)).build(), remote);
        gossiper.hearGossip(GossipMessage.newBuilder()
            .setMessageId(msgId1).setPayload(ByteString.copyFromUtf8(msgPayload2)).build(), remote);
        gossips.assertValueCount(1);
        assertTrue(gossips.values().get(0).getMessageId().equals(msgId1));
        assertTrue(gossips.values().get(0).getPayload().equals(ByteString.copyFromUtf8(msgPayload1)));
    }

    @Test
    public void testSweepGossip() {
        GossipMessage gossipMessage = GossipMessage.newBuilder()
            .setMessageId(msgId1).setPayload(ByteString.copyFromUtf8(msgPayload1)).build();
        GossipMessage gossipMessage2 = GossipMessage.newBuilder()
            .setMessageId(msgId2).setPayload(ByteString.copyFromUtf8(msgPayload2)).build();
        TestSubscriber<GossipMessage> gossips = TestSubscriber.create();
        gossiper.gossips()
            .subscribe(g -> gossips.onNext(g.value()));
        gossiper.hearGossip(gossipMessage, remote);
        gossiper.hearGossip(gossipMessage, remote);
        gossips.assertValueCount(1);

        int clusterSize = 4;
        int periodsToSweep = gossiper.gossipPeriodsToSweep(retransmitMultiplier, clusterSize);
        for (int i = 0; i < periodsToSweep; i++) {
            gossiper.nextPeriod(clusterSize);
        }
        // still in cache
        gossiper.hearGossip(gossipMessage, remote);
        gossips.assertValueCount(1);
        gossiper.hearGossip(gossipMessage2, remote);
        gossips.assertValueCount(2);
        // gossipMessage sweeped
        gossiper.nextPeriod(clusterSize);
        gossiper.hearGossip(gossipMessage, remote);
        gossips.assertValueCount(3);
    }

    @Test
    public void testConfirmGossip() {
        TestObserver<Timed<GossipMessage>> gossips = TestObserver.create();
        TestObserver<Duration> costs = new TestObserver<>();
        gossiper.gossips().subscribe(gossips);
        gossiper.generateGossip(ByteString.copyFromUtf8(msgPayload1)).thenAccept(costs::onNext);
        gossips.assertValueCount(1);

        int clusterSize = 4;
        int periodsToSpread = gossiper.gossipPeriodsToSpread(retransmitMultiplier, clusterSize);
        for (int i = 0; i < periodsToSpread + 1; i++) {
            gossiper.nextPeriod(clusterSize);
        }
        costs.assertValueCount(1);
        assertTrue(costs.values().get(0).toNanos() > 0);
    }

    @Test
    public void testConfirmGossipWithConfirmPeriodInterval() throws InterruptedException {
        TestObserver<Timed<MessageEnvelope>> testObserver = TestObserver.create();
        TestSubscriber<GossipMessage> gossips = TestSubscriber.create();
        gossiper.gossips()
            .subscribe(g -> gossips.onNext(g.value()));
        for (int i = 0; i < 10; i++) {
            gossiper.generateGossip(ByteString.copyFromUtf8(msgPayload1));
        }
        gossips.assertValueCount(10);

        int clusterSize = 8;
        List<GossipMessage> gossipMessages = gossiper.selectGossipsSendTo(remote, clusterSize);
        Assert.assertEquals(10, gossipMessages.size());
        int periodsToSpread = gossiper.gossipPeriodsToSpread(retransmitMultiplier, clusterSize);
        int periodsToSweep = gossiper.gossipPeriodsToSweep(retransmitMultiplier, clusterSize);
        assertTrue(periodsToSpread > 2);

        gossiper.nextPeriod(clusterSize);
        Assert.assertEquals(10, gossiper.selectGossipsSendTo(remote, clusterSize).size());
        testObserver.await((periodsToSweep / 2) * period.toMillis(), TimeUnit.MILLISECONDS);
        gossiper.nextPeriod(clusterSize);
        assertTrue(gossiper.selectGossipsSendTo(remote, clusterSize).size() < 10);
        assertTrue(gossiper.selectGossipsSendTo(remote, clusterSize).size() > 0);
    }

    @Test
    public void testConfirmGossipWithUnmoralPeriodInterval() throws InterruptedException {
        TestObserver<Timed<MessageEnvelope>> testObserver = TestObserver.create();
        TestSubscriber<GossipMessage> gossips = TestSubscriber.create();
        gossiper.gossips()
            .subscribe(g -> gossips.onNext(g.value()));
        for (int i = 0; i < 10; i++) {
            gossiper.generateGossip(ByteString.copyFromUtf8(msgPayload1));
        }
        gossips.assertValueCount(10);

        int clusterSize = 8;
        List<GossipMessage> gossipMessages = gossiper.selectGossipsSendTo(remote, clusterSize);
        Assert.assertEquals(10, gossipMessages.size());
        int periodsToSpread = gossiper.gossipPeriodsToSpread(retransmitMultiplier, clusterSize);
        int periodsToSweep = gossiper.gossipPeriodsToSweep(retransmitMultiplier, clusterSize);
        assertTrue(periodsToSpread > 2);

        gossiper.nextPeriod(clusterSize);
        Assert.assertEquals(10, gossiper.selectGossipsSendTo(remote, clusterSize).size());
        testObserver.await(periodsToSweep * period.toMillis(), TimeUnit.MILLISECONDS);
        gossiper.nextPeriod(clusterSize);
        Assert.assertEquals(0, gossiper.selectGossipsSendTo(remote, clusterSize).size());
    }

    @Test
    public void testSelectGossip() {
        GossipMessage gossipMessage = GossipMessage.newBuilder()
            .setMessageId(msgId1).setPayload(ByteString.copyFromUtf8(msgPayload1)).build();
        TestSubscriber<GossipMessage> gossips = TestSubscriber.create();
        gossiper.gossips()
            .subscribe(g -> gossips.onNext(g.value()));
        gossiper.hearGossip(gossipMessage, remote);
        gossips.assertValueCount(1);

        int clusterSize = 4;
        int periodsToSpread = gossiper.gossipPeriodsToSpread(retransmitMultiplier, clusterSize);
        // gossip origin and destination are the same
        List<GossipMessage> selectedGossips = gossiper.selectGossipsSendTo(remote, clusterSize);
        Assert.assertEquals(selectedGossips.size(), 0);

        for (int i = 0; i < periodsToSpread + 1; i++) {
            selectedGossips = gossiper.selectGossipsSendTo(remote2, clusterSize);
            Assert.assertEquals(selectedGossips.size(), 1);
            gossiper.nextPeriod(clusterSize);
        }
        selectedGossips = gossiper.selectGossipsSendTo(remote2, clusterSize);
        Assert.assertEquals(selectedGossips.size(), 0);
    }

}
