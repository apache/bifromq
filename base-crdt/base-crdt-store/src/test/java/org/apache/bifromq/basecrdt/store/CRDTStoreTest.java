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

package org.apache.bifromq.basecrdt.store;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertFalse;

import org.apache.bifromq.basecrdt.core.api.AWORSetOperation;
import org.apache.bifromq.basecrdt.core.api.CCounterOperation;
import org.apache.bifromq.basecrdt.core.api.CRDTURI;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.IAWORSet;
import org.apache.bifromq.basecrdt.core.api.ICCounter;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.store.annotation.StoreCfg;
import org.apache.bifromq.basecrdt.store.annotation.StoreCfgs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class CRDTStoreTest extends CRDTStoreTestTemplate {
    @StoreCfgs(stores = {
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test(groups = "integration")
    public void testAntiEntropy() {
        String setId = CRDTURI.toURI(CausalCRDTType.aworset, "set");
        IAWORSet set1 = storeMgr.host("s1", setId);
        IAWORSet set2 = storeMgr.host("s2", setId);
        IAWORSet set3 = storeMgr.host("s3", setId);
        ByteString value1 = copyFromUtf8("Value1");
        ByteString value2 = copyFromUtf8("Value2");
        ByteString value3 = copyFromUtf8("Value3");

        // build network
        storeMgr.join("s1", set1.id(), set1.id(), set2.id(), set3.id());
        storeMgr.join("s2", set2.id(), set1.id(), set2.id(), set3.id());
        storeMgr.join("s3", set3.id(), set1.id(), set2.id(), set3.id());

        set1.execute(AWORSetOperation.add(value1)).join();
        await().atMost(Duration.ofSeconds(5)).until(() -> set1.contains(value1));
        await().atMost(Duration.ofSeconds(5)).until(() -> set2.contains(value1));
        await().atMost(Duration.ofSeconds(5)).until(() -> set3.contains(value1));

        set2.execute(AWORSetOperation.add(value2)).join();
        await().atMost(Duration.ofSeconds(5)).until(() -> set1.contains(value2));
        await().atMost(Duration.ofSeconds(5)).until(() -> set2.contains(value2));
        await().atMost(Duration.ofSeconds(5)).until(() -> set3.contains(value2));

        set3.execute(AWORSetOperation.add(value3)).join();
        await().atMost(Duration.ofSeconds(5)).until(() -> set1.contains(value3));
        await().atMost(Duration.ofSeconds(5)).until(() -> set2.contains(value3));
        await().atMost(Duration.ofSeconds(5)).until(() -> set3.contains(value3));

        set1.execute(AWORSetOperation.clear()).join();
        await().atMost(Duration.ofSeconds(5)).until(set1::isEmpty);
        await().atMost(Duration.ofSeconds(5)).until(set2::isEmpty);
        await().atMost(Duration.ofSeconds(5)).until(set3::isEmpty);
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s0"),
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
        @StoreCfg(id = "s4"),
        @StoreCfg(id = "s5"),
        @StoreCfg(id = "s6"),
        @StoreCfg(id = "s7"),
        @StoreCfg(id = "s8"),
        @StoreCfg(id = "s9"),
        @StoreCfg(id = "s10"),
        @StoreCfg(id = "s11"),
        @StoreCfg(id = "s12"),
        @StoreCfg(id = "s13"),
        @StoreCfg(id = "s14"),
        @StoreCfg(id = "s15"),
        @StoreCfg(id = "s16"),
        @StoreCfg(id = "s17"),
        @StoreCfg(id = "s18"),
        @StoreCfg(id = "s19"),
    })
    @Test(groups = "integration")
    public void testMassiveNestedCounters() throws InterruptedException {
        int size = storeMgr.stores().size();
        int countersPerMap = 100;
        int countPerCounter = 100;
        CountDownLatch latch = new CountDownLatch(countPerCounter * countersPerMap * size);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
        String counterORMap = CRDTURI.toURI(CausalCRDTType.ormap, "counter");
        Replica[] replicaIds = new Replica[size];
        long[][] reads = new long[size][countersPerMap];
        Map<Replica, List<ICCounter>> ctrMap = Maps.newHashMap();
        AtomicBoolean inverseHappened = new AtomicBoolean();
        for (int i = 0; i < size; i++) {
            IORMap map = storeMgr.host("s" + i, counterORMap);
            Replica ctrMapR = map.id();
            replicaIds[i] = ctrMapR;
            reads[i] = new long[countersPerMap];
            ctrMap.computeIfAbsent(ctrMapR, k -> Lists.newArrayList());
            for (int c = 0; c < countersPerMap; c++) {
                ctrMap.get(ctrMapR).add(map.getCCounter(ByteString.copyFromUtf8("c_" + c)));
            }
        }
        for (int i = 0; i < size; i++) {
            storeMgr.join("s" + i, replicaIds[i], replicaIds);
        }
        long start = System.nanoTime();
        for (int i = 0; i < size; i++) {
            int j = i;
            class CounterAdder {
                final int mapIdx;
                final int ctrIdx;
                final ICCounter ctr;
                final long delay;
                volatile int count;

                CounterAdder(int mapIdx, int ctrIdx, ICCounter ctr, long delay) {
                    this.mapIdx = mapIdx;
                    this.ctrIdx = ctrIdx;
                    this.ctr = ctr;
                    this.delay = delay;
                }

                void start() {
                    if (count++ < countPerCounter) {
                        ctr.execute(CCounterOperation.add(1));
                        long read = reads[mapIdx][ctrIdx];
                        reads[mapIdx][ctrIdx] = ctr.read();
                        if (read > reads[mapIdx][ctrIdx]) {
                            inverseHappened.set(true);
                        }
                        latch.countDown();
                        executor.schedule(this::start, ThreadLocalRandom.current().nextLong(delay),
                            TimeUnit.MILLISECONDS);
                    }
                }
            }
            for (int c = 0; c < ctrMap.get(replicaIds[j]).size(); c++) {
                ICCounter cctr = ctrMap.get(replicaIds[j]).get(c);
                new CounterAdder(i, c, cctr, 10).start();
            }
        }

        latch.await();
        log.debug("Finish counting in {} ms", Duration.ofNanos(System.nanoTime() - start).toMillis());
        await().atMost(Duration.ofMillis(200000)).until(() -> {
            boolean pass = true;
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < ctrMap.get(replicaIds[i]).size(); j++) {
                    ICCounter ctr = ctrMap.get(replicaIds[i]).get(j);
                    long read = reads[i][j];
                    reads[i][j] = ctr.read();
                    if (read > reads[i][j]) {
                        inverseHappened.set(true);
                    }
                    if (ctr.read() != countPerCounter * size) {
                        pass = false;
                    }
                }
            }
            return pass;
        });
        assertFalse(inverseHappened.get());
        log.debug("Pass test in {} ms", Duration.ofNanos(System.nanoTime() - start).toMillis());
//        Thread.sleep(10000);
        executor.shutdownNow();
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s0", inflationInterval = 10),
        @StoreCfg(id = "s1", inflationInterval = 10),
        @StoreCfg(id = "s2", packetRandom = true),
        @StoreCfg(id = "s3", inflationInterval = 10),
        @StoreCfg(id = "s4", packetLossPercent = 0.1),
        @StoreCfg(id = "s5", packetDelayTime = 100),
        @StoreCfg(id = "s6"),
        @StoreCfg(id = "s7"),
        @StoreCfg(id = "s8"),
        @StoreCfg(id = "s9")
    })
    @Test(groups = "integration")
    public void testCCountersWithPacketLossAndReorder() throws InterruptedException {
        ccounterConcurrentWritingTest(5000);
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "ss0", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss1", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss2", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss3", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss4", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss5", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss6", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss7", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss8", inflationInterval = 10, packetRandom = false),
        @StoreCfg(id = "ss9", inflationInterval = 10, packetRandom = false)
    })
    @Test(groups = "integration")
    public void testCCounters() throws InterruptedException {
        ccounterConcurrentWritingTest(10000);
    }

    // This method is used to simulate the cluster locally
    private void ccounterConcurrentWritingTest(int countPerCounter) throws InterruptedException {
        List<String> storeIds = storeMgr.stores();
        CountDownLatch latch = new CountDownLatch(countPerCounter * storeIds.size());
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
        String ctrId = CRDTURI.toURI(CausalCRDTType.cctr, "counter");
        Replica[] replicaIds = new Replica[storeIds.size()];
        List<ICCounter> counters = new ArrayList<>(storeIds.size());
        AtomicInteger[] counts = new AtomicInteger[storeIds.size()];
        AtomicLong[] lastReads = new AtomicLong[storeIds.size()];
        for (int i = 0; i < storeIds.size(); i++) {
            ICCounter ctr = storeMgr.host(storeIds.get(i), ctrId);
            replicaIds[i] = ctr.id();
            counters.add(ctr);
            counts[i] = new AtomicInteger();
            lastReads[i] = new AtomicLong();
        }
        for (int i = 0; i < storeIds.size(); i++) {
            storeMgr.join(storeIds.get(i), replicaIds[i], replicaIds);
        }
        AtomicBoolean inverse = new AtomicBoolean(false);
        for (int i = 0; i < storeIds.size(); i++) {
            int j = i;
            executor.scheduleAtFixedRate(() -> {
                if (counts[j].getAndIncrement() < countPerCounter) {
                    counters.get(j).execute(CCounterOperation.add(1));
                    latch.countDown();
                    long r = lastReads[j].get();
                    lastReads[j].set(counters.get(j).read());
                    if (r > lastReads[j].get()) {
                        inverse.set(true);
                        log.debug("Counter {}: last={}, now={}, diff={}, inverse={}",
                            counters.get(j).id(), r, lastReads[j].get(), lastReads[j].get() - r,
                            r > lastReads[j].get());
                    }
                }
            }, 0, 1, TimeUnit.MILLISECONDS);
        }

        latch.await();
        log.debug("Finish counting");
        await().atMost(Duration.ofMillis(150000)).until(() -> {
            boolean pass = true;
            for (int i = 0; i < storeIds.size(); i++) {
                if (counters.get(i).read() != countPerCounter * storeIds.size()) {
                    log.debug("Counter[{}] reads:{}",
                        counters.get(i).id(), counters.get(i).read());
                    pass = false;
                }
            }
            return pass;
        });
        Assert.assertFalse(inverse.get());
        executor.shutdownNow();
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3")
    })
    @Test(groups = "integration")
    public void testNewReplicaJoin() {
        String crdtCRDT = CRDTURI.toURI(CausalCRDTType.aworset, "set");
        IAWORSet set1 = storeMgr.host("s1", crdtCRDT);
        Replica setR1 = set1.id();
        IAWORSet set2 = storeMgr.host("s2", crdtCRDT);
        Replica setR2 = set2.id();
        IAWORSet set3 = storeMgr.host("s3", crdtCRDT);
        Replica setR3 = set3.id();

        ByteString value1 = copyFromUtf8("Value1");
        ByteString value2 = copyFromUtf8("Value2");
        ByteString value3 = copyFromUtf8("Value3");

        // build two members network
        storeMgr.join("s1", setR1, setR1, setR2);
        storeMgr.join("s2", setR2, setR1, setR2);

        set1.execute(AWORSetOperation.add(value1)).join();
        await().until(() -> set1.contains(value1));
        await().until(() -> set2.contains(value1));

        set2.execute(AWORSetOperation.add(value2)).join();
        await().until(() -> set1.contains(value2));
        await().until(() -> set2.contains(value2));

        set3.execute(AWORSetOperation.add(value3)).join();
        await().until(() -> set3.contains(value3));

        // build three members network
        storeMgr.join("s1", setR1, setR1, setR2, setR3);
        storeMgr.join("s2", setR2, setR1, setR2, setR3);
        storeMgr.join("s3", setR3, setR1, setR2, setR3);
        await().until(() -> set1.contains(value3));
        await().until(() -> set2.contains(value3));

        await().until(() -> set3.contains(value1));
        await().until(() -> set3.contains(value2));
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test(groups = "integration")
    public void testMemberLeave() throws InterruptedException {
        String ctrId = CRDTURI.toURI(CausalCRDTType.cctr, "cctr");
        ICCounter c1 = storeMgr.host("s1", ctrId);
        Replica ctrR1 = c1.id();
        ICCounter c2 = storeMgr.host("s2", ctrId);
        Replica ctrR2 = c2.id();
        ICCounter c3 = storeMgr.host("s3", ctrId);
        Replica ctrR3 = c3.id();

        storeMgr.join("s1", ctrR1, ctrR1, ctrR2, ctrR3);
        storeMgr.join("s2", ctrR2, ctrR1, ctrR2, ctrR3);
        storeMgr.join("s3", ctrR3, ctrR1, ctrR2, ctrR3);

        c1.execute(CCounterOperation.add(10)).join();
        c2.execute(CCounterOperation.add(10)).join();
        c3.execute(CCounterOperation.add(10)).join();
        await().until(() -> c1.read() == 30);
        await().until(() -> c2.read() == 30);
        await().until(() -> c3.read() == 30);

        // s3 stopped gracefully
        storeMgr.stopStore("s3");
        // sleep for a while
        Thread.sleep(1000);

        // reset the cluster landscape of s1, s2
        storeMgr.join("s1", ctrR1, ctrR1, ctrR2);
        storeMgr.join("s2", ctrR2, ctrR1, ctrR2);

        // partition the state of s3 as well
//        storeMgr.partition("s1", ctrId, ctrR3.getId()).join();
        c1.execute(CCounterOperation.zeroOut(ctrR3.getId())).join();
//        storeMgr.partition("s2", ctrId, ctrR3.getId()).join();
        c2.execute(CCounterOperation.zeroOut(ctrR3.getId())).join();
        await().until(() -> c1.read() == 20);
        await().until(() -> c2.read() == 20);
    }

    @StoreCfgs(stores = {
        @StoreCfg(id = "s1"),
        @StoreCfg(id = "s2"),
        @StoreCfg(id = "s3"),
    })
    @Test(groups = "integration")
    public void testAutoHealWhenFalsePositiveNetworkPartition() throws InterruptedException {
        String clusterId = CRDTURI.toURI(CausalCRDTType.ormap, "ormap");
        IORMap c1 = storeMgr.host("s1", clusterId);
        Replica clusterR1 = c1.id();
        IORMap c2 = storeMgr.host("s2", clusterId);
        Replica clusterR2 = c2.id();
        IORMap c3 = storeMgr.host("s3", clusterId);
        Replica clusterR3 = c3.id();
        ByteString c1Addr = copyFromUtf8("ipaddr1");
        ByteString c2Addr = copyFromUtf8("ipaddr2");
        ByteString c3Addr = copyFromUtf8("ipaddr3");

        storeMgr.join("s1", clusterR1, clusterR1, clusterR2, clusterR3);
        storeMgr.join("s2", clusterR2, clusterR1, clusterR2, clusterR3);
        storeMgr.join("s3", clusterR3, clusterR1, clusterR2, clusterR3);

        c1.execute(ORMapOperation.update(clusterR1.getId()).with(MVRegOperation.write(c1Addr))).join();
        c2.execute(ORMapOperation.update(clusterR2.getId()).with(MVRegOperation.write(c2Addr))).join();
        c3.execute(ORMapOperation.update(clusterR3.getId()).with(MVRegOperation.write(c3Addr))).join();
        await().until(() -> Sets.newHashSet(c1.keys()).size() == 3);
        await().until(() -> Sets.newHashSet(c2.keys()).size() == 3);
        await().until(() -> Sets.newHashSet(c3.keys()).size() == 3);

        // sleep for a while to let anti-entropy between s1 and s2 become quiescent,
        // otherwise partition operation may not succeed within s1, s2
        Thread.sleep(1000);
        // simulate s3 is detected unreachable wrongly from s1, s2's failure detector,
        // partition s3's state from their respective local state proactively
//        storeMgr.partition("s1", clusterId, clusterR3.getId()).join();
        c1.execute(ORMapOperation.remove(clusterR3.getId()).of(CausalCRDTType.mvreg)).join();
//        storeMgr.partition("s2", clusterId, clusterR3.getId()).join();
        c2.execute(ORMapOperation.remove(clusterR3.getId()).of(CausalCRDTType.mvreg)).join();
        await().until(() -> Sets.newHashSet(c1.keys()).size() == 2);
        await().until(() -> Sets.newHashSet(c2.keys()).size() == 2);

        // and add s3 to local-split-brain resolve list and initialize split-brain resolving process
        storeMgr.join("s1", clusterR1, clusterR1, clusterR2);
        storeMgr.join("s1", clusterR1, clusterR1, clusterR2, clusterR3);

        storeMgr.join("s2", clusterR2, clusterR1, clusterR2);
        storeMgr.join("s2", clusterR2, clusterR1, clusterR2, clusterR3);
        // changes to s1, s2 will propagate to s3
        await().until(() -> Sets.newHashSet(c3.keys()).size() == 2);

        // s3 acknowledged it's been detected as a failed member in the current cluster, so it refutes
        c3.execute(ORMapOperation.update(clusterR3.getId()).with(MVRegOperation.write(c3Addr))).join();

        // by either forcing sync with detecting members or
//        storeMgr.sync("s3", clusterId, clusterR1.getId());
//        storeMgr.sync("s3", clusterId, clusterR2.getId());

        // 1) leave the cluster(by joining a single member cluster)
//        storeMgr.join("s3", clusterId, clusterR3.getId(), clusterR3.getId());
        // 2) join to the original cluster
//        storeMgr.join("s3", clusterId, clusterR3.getId(), clusterR1.getId(), clusterR2.getId(), clusterR3.getId());
        // NOTE:
        // state from s3 is recovered in s1, s2, split brain healed
        await().until(() -> Sets.newHashSet(c1.keys()).size() == 3);
        await().until(() -> Sets.newHashSet(c2.keys()).size() == 3);
        await().until(() -> Sets.newHashSet(c3.keys()).size() == 3);
    }
}
