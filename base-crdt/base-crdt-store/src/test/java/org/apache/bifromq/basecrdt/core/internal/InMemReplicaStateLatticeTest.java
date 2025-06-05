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

package org.apache.bifromq.basecrdt.core.internal;

import static org.apache.bifromq.basecrdt.core.internal.EventHistoryUtil.isRemembering;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.dot;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.replacement;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.replacements;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.singleDot;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecrdt.proto.Replacement;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.proto.StateLattice;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InMemReplicaStateLatticeTest {
    private InMemReplicaStateLattice testLattice;
    private final Replica ownerReplica = Replica.newBuilder().setId(copyFromUtf8("Owner")).build();
    private final ByteString replicaA = copyFromUtf8("A");
    private final ByteString replicaB = copyFromUtf8("B");

    @BeforeMethod
    public void setup() {
        testLattice = new InMemReplicaStateLattice(ownerReplica, Duration.ofMillis(1000), Duration.ofMillis(200));
        assertFalse(testLattice.lattices().hasNext());
    }

    @Test
    public void testJoin() {
        List<Replacement> states = newArrayList(
            replacement(dot(replicaA, 1, singleDot(replicaA, 1))),
            replacement(dot(replicaA, 2, singleDot(replicaA, 2))),
            replacement(dot(replicaA, 3, singleDot(replicaA, 3))),
            replacement(dot(replicaA, 5, singleDot(replicaA, 5))),
            replacement(dot(replicaA, 7, singleDot(replicaA, 7))),
            replacement(dot(replicaA, 8, singleDot(replicaA, 8)))
        );
        IReplicaStateLattice.JoinDiff diff = testLattice.join(states);
        Set<StateLattice> toMatch = Sets.newHashSet(
            singleDot(replicaA, 1),
            singleDot(replicaA, 2),
            singleDot(replicaA, 3),
            singleDot(replicaA, 5),
            singleDot(replicaA, 7),
            singleDot(replicaA, 8)
        );
        assertEquals(newHashSet(diff.adds()), toMatch);
        assertFalse(diff.removes().iterator().hasNext());
        TestUtil.assertUnorderedSame(toMatch.iterator(), testLattice.lattices());

        // join again nothing happened
        diff = testLattice.join(states);
        assertFalse(diff.adds().iterator().hasNext());
        assertFalse(diff.removes().iterator().hasNext());

        assertEquals(testLattice.latticeIndex(), TestUtil.toLatticeEvents(replicaA, 1, 3, 5, 5, 7, 8));
        assertTrue(testLattice.historyIndex().isEmpty());
    }

    @Test
    public void testJoinWithReplacementSemantic() {
        testLattice.join(singleton(replacement(dot(replicaA, 1, singleDot(replicaA, 1)))));
        testLattice.join(singleton(replacement(dot(replicaA, 3, singleDot(replicaA, 3)), dot(replicaA, 2))));
        testLattice.join(replacements(dot(replicaA, 4, singleDot(replicaA, 4)), singleton(dot(replicaA, 1))));
        testLattice.join(replacements(dot(replicaA, 4, singleDot(replicaA, 4)), singleton(dot(replicaA, 3))));
        assertEquals(testLattice.lattices().next(), singleDot(replicaA, 4));
        Optional<Iterable<Replacement>> deltaLattice = testLattice
            .delta(TestUtil.toLatticeEvents(replicaA, 1, 3), emptyMap(), 10);
        assertTrue(deltaLattice.isPresent());
        assertEquals(deltaLattice.get(), Sets.newHashSet(replacements(dot(replicaA, 4, singleDot(replicaA, 4)),
            newHashSet(dot(replicaA, 3), dot(replicaA, 1)))));
    }

    @Test
    public void testJoinWithRemovalHistory() {
        List<Replacement> states = newArrayList(
            replacement(dot(replicaA, 1, singleDot(replicaA, 1))),
            replacement(dot(replicaA, 2, singleDot(replicaA, 2))),
            replacement(dot(replicaA, 3, singleDot(replicaA, 3))),
            replacement(dot(replicaA, 5, singleDot(replicaA, 5))),
            replacement(dot(replicaA, 7, singleDot(replicaA, 7))),
            replacement(dot(replicaA, 8, singleDot(replicaA, 8))));

        testLattice.join(states);
        List<Replacement> removalDelta = newArrayList(
            replacement(dot(replicaA, 1)),
            replacement(dot(replicaA, 2)),
            replacement(dot(replicaA, 3)));
        IReplicaStateLattice.JoinDiff diff = testLattice.join(removalDelta);
        assertFalse(diff.adds().iterator().hasNext());
        assertEquals(newHashSet(diff.removes()), Sets.newHashSet(
            singleDot(replicaA, 1),
            singleDot(replicaA, 2),
            singleDot(replicaA, 3)
        ));
        TestUtil.assertSame(newArrayList(singleDot(replicaA, 5), singleDot(replicaA, 7), singleDot(replicaA, 8))
            .iterator(), testLattice.lattices());

        assertEquals(testLattice.latticeIndex(), TestUtil.toLatticeEvents(replicaA, 5, 5, 7, 8));

        assertEquals(testLattice.historyIndex(), TestUtil.toLatticeEvents(replicaA, 1, 3));

        // remove again
        diff = testLattice.join(removalDelta);
        assertFalse(diff.adds().iterator().hasNext());
        assertFalse(diff.removes().iterator().hasNext());
        // join removed states before removalHistory being forgotten
        diff = testLattice.join(states);
        assertFalse(diff.adds().iterator().hasNext());
        assertFalse(diff.removes().iterator().hasNext());
    }

    @Test
    public void testJoinAfterRemovalHistoryExpiry() throws InterruptedException {
        IReplicaStateLattice.JoinDiff diff = testLattice
            .join(newArrayList(
                replacement(dot(replicaA, 1)),
                replacement(dot(replicaA, 2)),
                replacement(dot(replicaA, 3)))
            );
        // compare history
        assertTrue(testLattice.latticeIndex().isEmpty());
        assertEquals(testLattice.historyIndex(), TestUtil.toLatticeEvents(replicaA, 1, 3));

        assertFalse(diff.adds().iterator().hasNext());
        assertFalse(diff.removes().iterator().hasNext());
        List<Replacement> states = newArrayList(
            replacement(dot(replicaA, 1, singleDot(replicaA, 1))),
            replacement(dot(replicaA, 2, singleDot(replicaA, 2))),
            replacement(dot(replicaA, 3, singleDot(replicaA, 3)))
        );
        // join removed states before removalHistory being forgotten
        diff = testLattice.join(states);
        assertFalse(diff.adds().iterator().hasNext());
        assertFalse(diff.removes().iterator().hasNext());
        // no change to history
        assertTrue(testLattice.latticeIndex().isEmpty());
        assertEquals(testLattice.historyIndex(), TestUtil.toLatticeEvents(replicaA, 1, 3));
        // sleep until removalHistory being forgotten
        Thread.sleep(3000);
        testLattice.compact();
        assertTrue(testLattice.historyIndex().isEmpty());

        diff = testLattice.join(states);
        TestUtil.assertUnorderedSame(newArrayList(
                singleDot(replicaA, 1),
                singleDot(replicaA, 2),
                singleDot(replicaA, 3))
                .iterator(),
            diff.adds().iterator());
        assertEquals(testLattice.latticeIndex(), TestUtil.toLatticeEvents(replicaA, 1, 3));
        assertTrue(testLattice.historyIndex().isEmpty());
    }

    @Test
    public void testIndexChangeAfterJoinRemoveHistory() {
        List<Replacement> states = newArrayList(
            replacement(dot(replicaA, 1, singleDot(replicaA, 1))),
            replacement(dot(replicaA, 2, singleDot(replicaA, 2))),
            replacement(dot(replicaA, 3, singleDot(replicaA, 3))));
        testLattice.join(states);
        assertEquals(testLattice.latticeIndex().keySet(), Sets.<ByteString>newHashSet(replicaA));
        assertTrue(testLattice.historyIndex().isEmpty());

        testLattice.join(newArrayList(
            replacement(dot(replicaA, 1)),
            replacement(dot(replicaA, 2)),
            replacement(dot(replicaA, 3)))
        );
        assertTrue(testLattice.latticeIndex().isEmpty());
        assertEquals(testLattice.historyIndex().keySet(), Sets.<ByteString>newHashSet(replicaA));

        // join again before history expire
        testLattice.join(states);
        assertTrue(testLattice.latticeIndex().isEmpty());
    }

    @Test
    public void testDelta() {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 1, singleDot(replicaA, 1))),
            replacement(dot(replicaA, 2, singleDot(replicaA, 2))),
            replacement(dot(replicaA, 3, singleDot(replicaA, 3))),
            replacement(dot(replicaA, 5, singleDot(replicaA, 5))),
            replacement(dot(replicaA, 7, singleDot(replicaA, 7))),
            replacement(dot(replicaA, 8, singleDot(replicaA, 8))),
            replacement(dot(replicaB, 1, singleDot(replicaA, 1))),
            replacement(dot(replicaB, 2, singleDot(replicaA, 2))),
            replacement(dot(replicaB, 3, singleDot(replicaA, 3))),
            replacement(dot(replicaB, 5, singleDot(replicaA, 5))),
            replacement(dot(replicaB, 7, singleDot(replicaA, 7))),
            replacement(dot(replicaB, 8, singleDot(replicaA, 8)))
        );

        testLattice.join(states);
        // no contributor to compare

        assertEquals(newHashSet(testLattice.delta(emptyMap(), emptyMap(), 100).get()), states);
        //
        assertEquals(newHashSet(testLattice.delta(TestUtil.toLatticeEvents(replicaB), emptyMap(), 100).get()), states);

        assertEquals(newHashSet(
            testLattice.delta(TestUtil.toLatticeEvents(replicaB, 1, 8), TestUtil.toLatticeEvents(replicaA, 1, 3, 5, 5),
                100).get()
        ), newHashSet(
            replacement(dot(replicaA, 7, singleDot(replicaA, 7))),
            replacement(dot(replicaA, 8, singleDot(replicaA, 8)))
        ));

        // limited history
        Optional<Iterable<Replacement>> deltaProto =
            testLattice.delta(TestUtil.toLatticeEvents(replicaA), emptyMap(), 3);
        assertTrue(deltaProto.isPresent());
        assertEquals(Sets.newHashSet(deltaProto.get()).size(), 3);
        // oa history to exclude
        deltaProto = testLattice.delta(TestUtil.toLatticeEvents(replicaA, 1, 2, 5, 5), emptyMap(), 10);
        assertTrue(deltaProto.isPresent());
        TestUtil.assertUnorderedSame(newHashSet(
                replacement(dot(replicaA, 3, singleDot(replicaA, 3))),
                replacement(dot(replicaA, 7, singleDot(replicaA, 7))),
                replacement(dot(replicaA, 8, singleDot(replicaA, 8))),
                replacement(dot(replicaB, 1, singleDot(replicaA, 1))),
                replacement(dot(replicaB, 2, singleDot(replicaA, 2))),
                replacement(dot(replicaB, 3, singleDot(replicaA, 3))),
                replacement(dot(replicaB, 5, singleDot(replicaA, 5))),
                replacement(dot(replicaB, 7, singleDot(replicaA, 7))),
                replacement(dot(replicaB, 8, singleDot(replicaA, 8)))).iterator(),
            deltaProto.get().iterator());

        // oa history to exclude with limited events
        deltaProto = testLattice.delta(TestUtil.toLatticeEvents(replicaA, 1, 2), emptyMap(), 2);
        assertTrue(deltaProto.isPresent());
        assertEquals(Sets.newHashSet(deltaProto.get()).size(), 2);
        assertFalse(Sets.newHashSet(deltaProto.get()).contains(dot(replicaA, 1, singleDot(replicaA, 1))));
        assertFalse(Sets.newHashSet(deltaProto.get()).contains(dot(replicaA, 2, singleDot(replicaA, 2))));
    }

    @Test
    public void testDeltaWithCoveredHistory() {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 3, singleDot(replicaA, 3)), dot(replicaA, 2), dot(replicaA, 1)),
            replacement(dot(replicaA, 5, singleDot(replicaA, 5))),
            replacement(dot(replicaA, 8, singleDot(replicaA, 8)), dot(replicaA, 7), dot(replicaA, 6))
        );
        testLattice.join(states);

        assertEquals(newHashSet(
                testLattice.delta(TestUtil.toLatticeEvents(replicaA), TestUtil.toLatticeEvents(replicaA, 1, 3), 100).get()
            ),
            newHashSet(
                replacement(dot(replicaA, 5, singleDot(replicaA, 5))),
                replacement(dot(replicaA, 8, singleDot(replicaA, 8)), dot(replicaA, 7), dot(replicaA, 6))
            ));

        assertEquals(newHashSet(
                testLattice.delta(TestUtil.toLatticeEvents(replicaA, 5, 5), TestUtil.toLatticeEvents(replicaA, 1, 3, 6, 7),
                    100).get()
            ),
            newHashSet(
                replacement(dot(replicaA, 8, singleDot(replicaA, 8)), dot(replicaA, 7))
            ));

        assertFalse(
            testLattice.delta(TestUtil.toLatticeEvents(replicaA, 1, 3, 5, 5, 6, 8), emptyMap(), 100).isPresent());

        assertFalse(
            testLattice.delta(emptyMap(), TestUtil.toLatticeEvents(replicaA, 1, 3, 5, 5, 6, 8), 100).isPresent());

        assertFalse(testLattice.delta(TestUtil.toLatticeEvents(replicaA, 5, 5),
            TestUtil.toLatticeEvents(replicaA, 1, 3, 6, 8), 100).isPresent());
    }

    @Test
    public void testDeltaWithLimit() {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 8, singleDot(replicaA, 8)),
                dot(replicaA, 7),
                dot(replicaA, 6),
                dot(replicaA, 5))
        );
        testLattice.join(states);
        Optional<Iterable<Replacement>> deltaProto =
            testLattice.delta(TestUtil.toLatticeEvents(replicaA, 1, 4), emptyMap(), 2);
        assertTrue(deltaProto.isPresent());
        assertEquals(deltaProto.get(), states);

        deltaProto = testLattice.delta(TestUtil.toLatticeEvents(replicaA, 1, 5), emptyMap(), 2);
        assertTrue(deltaProto.isPresent());
        assertEquals(deltaProto.get(), states);

        deltaProto = testLattice.delta(TestUtil.toLatticeEvents(replicaA, 1, 7), emptyMap(), 2);
        assertTrue(deltaProto.isPresent());
        assertEquals(deltaProto.get(),
            newHashSet(replacement(dot(replicaA, 8, singleDot(replicaA, 8)), dot(replicaA, 7))));
    }

    @Test
    public void testDeltaOfHistoryOnly() {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 8),
                dot(replicaA, 7),
                dot(replicaA, 6),
                dot(replicaA, 5))
        );
        testLattice.join(states);
        Optional<Iterable<Replacement>> deltaProto =
            testLattice.delta(TestUtil.toLatticeEvents(replicaA, 5, 5), emptyMap(), 2);
        assertTrue(deltaProto.isPresent());
        assertEquals(deltaProto.get(), states);

        deltaProto = testLattice.delta(TestUtil.toLatticeEvents(replicaA, 6, 6), emptyMap(), 2);
        assertTrue(deltaProto.isPresent());
        assertEquals(deltaProto.get(),
            newHashSet(replacement(dot(replicaA, 8), dot(replicaA, 7), dot(replicaA, 6))));

        deltaProto = testLattice.delta(TestUtil.toLatticeEvents(replicaA, 7, 7), emptyMap(), 2);
        assertTrue(deltaProto.isPresent());
        assertEquals(deltaProto.get(), newHashSet(replacement(dot(replicaA, 8), dot(replicaA, 7))));
    }

    @Test
    public void testDeltaWithRemovalHistory() throws InterruptedException {
        testLattice.join(newArrayList(
            replacement(dot(replicaA, 1)),
            replacement(dot(replicaA, 2)),
            replacement(dot(replicaA, 3)))
        );
        Optional<Iterable<Replacement>> deltaProto =
            testLattice.delta(TestUtil.toLatticeEvents(replicaA, 1, 2), emptyMap(), 10);
        assertTrue(deltaProto.isPresent());
        assertEquals(newHashSet(deltaProto.get()), newHashSet(replacement(dot(replicaA, 2)),
            replacement(dot(replicaA, 1))));
        // wait for forgetting
        Thread.sleep(3000);
        testLattice.compact();
        deltaProto = testLattice.delta(TestUtil.toLatticeEvents(replicaA, 1, 2), emptyMap(), 10);
        assertFalse(deltaProto.isPresent());
    }

    @Test
    public void testCompact1() throws InterruptedException {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 1, singleDot(replicaA, 1)))
        );
        testLattice.join(states);
        // l(1)
        assertTrue(isRemembering(testLattice.latticeIndex(), replicaA, 1));
        Thread.sleep(1100);
        assertFalse(testLattice.compact());
        assertTrue(isRemembering(testLattice.latticeIndex(), replicaA, 1));
    }

    @Test
    public void testCompact2() throws InterruptedException {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 1))
        );
        testLattice.join(states);
        // h(1)
        assertFalse(isRemembering(testLattice.latticeIndex(), replicaA, 1));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 1));
        Thread.sleep(1100);
        // h(1,exp)
        assertFalse(testLattice.compact());
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 1));
    }

    @Test
    public void testCompact3() throws InterruptedException {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 1))
        );
        testLattice.join(states);
        // h(1)
        Thread.sleep(1100);
        states = newHashSet(
            replacement(dot(replicaA, 2), dot(replicaA, 1))
        );
        testLattice.join(states);
        // h(2) -> h(1,exp)
        assertTrue(testLattice.compact());
        // h(2) -> h(1,exp)
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 1));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 2));

        Thread.sleep(1100);
        // h(2,exp) -> h(1,exp)
        assertFalse(testLattice.compact());
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 1));
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 2));
    }

    @Test
    public void testCompact4() throws InterruptedException {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 2, singleDot(replicaA, 2)), dot(replicaA, 1))
        );

        testLattice.join(states);
        // l(2) -> h(1)
        assertTrue(isRemembering(testLattice.latticeIndex(), replicaA, 2));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 1));
        Thread.sleep(1100);
        // l(2) -> h(1,exp)
        assertFalse(testLattice.compact());
        // l(2) -> h(1,exp)
        assertTrue(isRemembering(testLattice.latticeIndex(), replicaA, 2));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 1));
    }

    @Test
    public void testCompact5() throws InterruptedException {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 3, singleDot(replicaA, 3)), dot(replicaA, 2), dot(replicaA, 1))
        );
        testLattice.join(states);
        // l(3) -> h(2) -> h(1)
        assertTrue(isRemembering(testLattice.latticeIndex(), replicaA, 3));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 2));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 1));
        Thread.sleep(1100);
        // l(3) -> h(2,exp) -> h(1,exp)
        assertFalse(testLattice.compact());
        // l(3) -> h(2,exp)
        assertTrue(isRemembering(testLattice.latticeIndex(), replicaA, 3));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 2));
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 1));
    }

    @Test
    public void testCompact6() throws InterruptedException {
        Set<Replacement> states = newHashSet(replacement(dot(replicaA, 1)));
        testLattice.join(states);
        Thread.sleep(1100);
        states = newHashSet(replacement(dot(replicaA, 2), dot(replicaA, 1)));
        testLattice.join(states);
        Thread.sleep(1100);
        states = newHashSet(replacement(dot(replicaA, 3), dot(replicaA, 2)));
        testLattice.join(states);

        // h(3) -> h(2,exp) -> h(1,exp)
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 3));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 2));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 1));

        assertTrue(testLattice.compact());
        // h(3) -> h(2,exp)
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 3));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 2));
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 1));

        // h(3) -> h(2,exp)
        assertTrue(testLattice.compact());
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 3));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 2));
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 1));

        Thread.sleep(1100);
        // h(3,exp) -> h(2,exp)
        assertFalse(testLattice.compact());
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 3));
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 2));
    }

    @Test
    public void compact7() throws InterruptedException {
        Set<Replacement> states = newHashSet(
            replacement(dot(replicaA, 4, singleDot(replicaA, 4)), dot(replicaA, 3), dot(replicaA, 1)),
            replacement(dot(replicaA, 2), dot(replicaA, 1)));
        testLattice.join(states);
        // l(4) -> h(3) -> h(1)
        //         h(2) -> h(1)
        assertTrue(isRemembering(testLattice.latticeIndex(), replicaA, 4));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 3));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 2));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 1));
        Thread.sleep(1100);
        // l(4) -> h(3,exp) -> h(1,exp)
        //         h(2,exp) -> h(1,exp)
        assertFalse(testLattice.compact());
        // l(4) -> h(3,exp)
        assertTrue(isRemembering(testLattice.latticeIndex(), replicaA, 4));
        assertTrue(isRemembering(testLattice.historyIndex(), replicaA, 3));
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 2));
        assertFalse(isRemembering(testLattice.historyIndex(), replicaA, 1));
    }
}
