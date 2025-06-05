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

import static org.apache.bifromq.basecrdt.core.api.CRDTURI.toURI;
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.cctr;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecrdt.core.api.CCounterOperation;
import org.apache.bifromq.basecrdt.core.api.ICCounter;
import org.apache.bifromq.basecrdt.proto.Replacement;
import org.apache.bifromq.basecrdt.proto.Replica;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.observers.TestObserver;
import java.time.Duration;
import java.util.Optional;
import org.testng.annotations.Test;

public class CCounterTest extends CRDTTest {
    private final Replica leftReplica = Replica.newBuilder()
        .setUri(toURI(cctr, "ccounter"))
        .setId(ByteString.copyFromUtf8("left-address"))
        .build();
    private final Replica rightReplica = Replica.newBuilder()
        .setUri(toURI(cctr, "ccounter"))
        .setId(ByteString.copyFromUtf8("right-address"))
        .build();

    @Test
    public void testOperation() {
        CCounterInflater cctrInflater = new CCounterInflater(leftReplica,
            newStateLattice(leftReplica, 1000), executor, Duration.ofMillis(100));
        ICCounter cctr = cctrInflater.getCRDT();
        assertEquals(cctr.id(), leftReplica);

        assertEquals(cctr.read(), 0);

        cctr.execute(CCounterOperation.add(10)).join();
        assertEquals(cctr.read(), 10);
        cctr.execute(CCounterOperation.add(10)).join();
        assertEquals(cctr.read(), 20);

        cctr.execute(CCounterOperation.preset(10)).join();
        assertEquals(cctr.read(), 10);

        cctr.execute(CCounterOperation.zeroOut()).join();
        assertEquals(cctr.read(), 0);

        // batch execute
        cctr.execute(CCounterOperation.add(10));
        cctr.execute(CCounterOperation.zeroOut());
        cctr.execute(CCounterOperation.add(10)).join();
        assertEquals(cctr.read(), 10);
    }

    @Test
    public void testJoin() {
        CCounterInflater leftInflater = new CCounterInflater(leftReplica,
            newStateLattice(leftReplica, 100000), executor, Duration.ofMillis(100));
        ICCounter left = leftInflater.getCRDT();

        CCounterInflater rightInflater = new CCounterInflater(rightReplica,
            newStateLattice(rightReplica, 100000), executor, Duration.ofMillis(100));
        ICCounter right = rightInflater.getCRDT();

        left.execute(CCounterOperation.add(10));
        left.execute(CCounterOperation.add(10));
        left.execute(CCounterOperation.add(10)).join();

        sync(leftInflater, rightInflater);
        assertEquals(right.read(), 30);

        // following codes simulate OR history broadcast during anti-entropy
        TestObserver<Long> inflationObserver = new TestObserver<>();
        right.inflation().subscribe(inflationObserver);
        right.execute(CCounterOperation.add(50));
        right.execute(CCounterOperation.zeroOut()).join();
        assertTrue(inflationObserver.values().isEmpty());
        sync(leftInflater, rightInflater);
        assertEquals(left.read(), 30);
        assertEquals(right.read(), 30);
    }

    @Test
    public void testZeroOut() {
        CCounterInflater leftInflater = new CCounterInflater(leftReplica,
            newStateLattice(leftReplica, 100000), executor, Duration.ofMillis(100));
        ICCounter left = leftInflater.getCRDT();

        CCounterInflater rightInflater = new CCounterInflater(rightReplica,
            newStateLattice(rightReplica, 100000), executor, Duration.ofMillis(100));
        ICCounter right = rightInflater.getCRDT();

        left.execute(CCounterOperation.add(10));
        left.execute(CCounterOperation.add(10));
        left.execute(CCounterOperation.add(10)).join();

        sync(leftInflater, rightInflater);
        assertEquals(right.read(), 30);

        // after partition left contribution is discarded locally but don't keep the removal history
        right.execute(CCounterOperation.zeroOut(leftReplica.getId())).join();
        assertEquals(right.read(), 0);

        // sync again will recover left-contributed state in right replica
        left.execute(CCounterOperation.add(10)).join();
        assertEquals(left.read(), 40);
        sync(leftInflater, rightInflater);
        assertEquals(right.read(), 40);
    }

    @Test
    public void testZeroOutInBatch() {
        CCounterInflater leftInflater = new CCounterInflater(leftReplica,
            newStateLattice(leftReplica, 100000), executor, Duration.ofMillis(100));
        ICCounter left = leftInflater.getCRDT();

        CCounterInflater rightInflater = new CCounterInflater(rightReplica,
            newStateLattice(rightReplica, 100000), executor, Duration.ofMillis(100));
        ICCounter right = rightInflater.getCRDT();

        left.execute(CCounterOperation.add(10)).join();

        sync(leftInflater, rightInflater);
        assertEquals(right.read(), 10);

        left.execute(CCounterOperation.add(10)).join();
        Optional<Iterable<Replacement>> deltaProto =
            leftInflater.delta(
                rightInflater.latticeEvents(),
                rightInflater.historyEvents(),
                Integer.MAX_VALUE).join();
        rightInflater.join(deltaProto.get()).join();
        right.execute(CCounterOperation.zeroOut(leftReplica.getId())).join();

        assertEquals(right.read(), 0);
        // sync again will recover left-contributed state in right replica
        left.execute(CCounterOperation.add(10)).join();
        sync(leftInflater, rightInflater);
        assertEquals(left.read(), 30);
        assertEquals(right.read(), 30);
    }
}
