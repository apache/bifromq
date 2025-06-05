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
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.mvreg;
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.ormap;
import static java.util.Collections.emptySet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecrdt.core.api.AWORSetOperation;
import org.apache.bifromq.basecrdt.core.api.CCounterOperation;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.DWFlagOperation;
import org.apache.bifromq.basecrdt.core.api.EWFlagOperation;
import org.apache.bifromq.basecrdt.core.api.IAWORSet;
import org.apache.bifromq.basecrdt.core.api.ICCounter;
import org.apache.bifromq.basecrdt.core.api.IDWFlag;
import org.apache.bifromq.basecrdt.core.api.IEWFlag;
import org.apache.bifromq.basecrdt.core.api.IMVReg;
import org.apache.bifromq.basecrdt.core.api.IORMap;
import org.apache.bifromq.basecrdt.core.api.IRWORSet;
import org.apache.bifromq.basecrdt.core.api.MVRegOperation;
import org.apache.bifromq.basecrdt.core.api.ORMapOperation;
import org.apache.bifromq.basecrdt.core.api.RWORSetOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.disposables.Disposable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class ORMapTest extends CRDTTest {
    private final Replica leftReplica = Replica.newBuilder()
        .setUri(toURI(ormap, "ormap"))
        .setId(ByteString.copyFromUtf8("left-address"))
        .build();
    private final Replica rightReplica = Replica.newBuilder()
        .setUri(toURI(ormap, "ormap"))
        .setId(ByteString.copyFromUtf8("right-address"))
        .build();
    private final ByteString key1 = ByteString.copyFromUtf8("key1");
    private final ByteString key2 = ByteString.copyFromUtf8("key1");
    private final ByteString key1_1 = ByteString.copyFromUtf8("key1_1");
    private final ByteString key1_2 = ByteString.copyFromUtf8("key1_2");

    private final ByteString elem1 = ByteString.copyFromUtf8("e1");
    private final ByteString elem2 = ByteString.copyFromUtf8("e2");
    private final ByteString elem3 = ByteString.copyFromUtf8("e3");


    @Test
    public void testOperation() {
        ORMapInflater orMapInflater = new ORMapInflater(leftReplica, newStateLattice(leftReplica, 1000),
            executor, Duration.ofMillis(100));
        IORMap ormap = orMapInflater.getCRDT();
        assertEquals(ormap.id(), leftReplica);

        {
            // sub ormap
            IORMap subMap = ormap.getORMap(key1);
            IAWORSet subSubSet = subMap.getAWORSet(key1_1);
            ormap.execute(ORMapOperation.update(key1, key1_1).with(AWORSetOperation.add(elem1)));
            subSubSet.execute(AWORSetOperation.add(elem2)).join();
            assertTrue(subSubSet.contains(elem1));
            assertTrue(subSubSet.contains(elem2));

            List<IORMap.ORMapKey> keys = Lists.newArrayList(ormap.keys());
            assertEquals(keys.size(), 1);
            assertEquals(keys.get(0).key(), key1);
            assertEquals(keys.get(0).valueType(), CausalCRDTType.ormap);

            keys = Lists.newArrayList(subMap.keys());
            assertEquals(keys.size(), 1);
            assertEquals(keys.get(0).key(), key1_1);
            assertEquals(keys.get(0).valueType(), CausalCRDTType.aworset);


            ormap.execute(ORMapOperation.remove(key1).of(CausalCRDTType.ormap)).join();
            assertFalse(subSubSet.contains(elem1));
            assertFalse(subSubSet.contains(elem2));

            keys = Lists.newArrayList(ormap.keys());
            assertEquals(keys.size(), 0);

            keys = Lists.newArrayList(subMap.keys());
            assertEquals(keys.size(), 0);
        }

        {
            // sub aworset
            IAWORSet subSet = ormap.getAWORSet(key1);
            ormap.execute(ORMapOperation.update(key1).with(AWORSetOperation.add(elem1)));
            subSet.execute(AWORSetOperation.add(elem2)).join();
            assertTrue(subSet.contains(elem1));
            assertTrue(subSet.contains(elem2));
            ormap.execute(ORMapOperation.update(key1).with(AWORSetOperation.remove(elem1))).join();
            assertFalse(subSet.contains(elem1));
            subSet.execute(AWORSetOperation.remove(elem2)).join();
            assertFalse(subSet.contains(elem2));
        }

        {
            // sub rworset
            IRWORSet subSet = ormap.getRWORSet(key1);
            ormap.execute(ORMapOperation.update(key1).with(RWORSetOperation.add(elem1)));
            subSet.execute(RWORSetOperation.add(elem2)).join();
            assertTrue(subSet.contains(elem1));
            assertTrue(subSet.contains(elem2));
            ormap.execute(ORMapOperation.update(key1).with(RWORSetOperation.remove(elem1))).join();
            assertFalse(subSet.contains(elem1));
            subSet.execute(RWORSetOperation.remove(elem2)).join();
            assertFalse(subSet.contains(elem2));
        }

        {
            // sub ccounter
            ICCounter subCounter = ormap.getCCounter(key1);
            ormap.execute(ORMapOperation.update(key1).with(CCounterOperation.add(10)));
            subCounter.execute(CCounterOperation.add(10)).join();
            assertEquals(subCounter.read(), 20);
            ormap.execute(ORMapOperation.update(key1).with(CCounterOperation.zeroOut())).join();
            assertEquals(subCounter.read(), 0);
        }

        {
            // sub mvreg
            IMVReg subMVReg = ormap.getMVReg(key1);
            ormap.execute(ORMapOperation.update(key1).with(MVRegOperation.write(elem1))).join();
            assertEquals(Sets.newHashSet(subMVReg.read()), Sets.<ByteString>newHashSet(elem1));
            subMVReg.execute(MVRegOperation.reset()).join();
            assertEquals(Sets.newHashSet(subMVReg.read()), emptySet());
            subMVReg.execute(MVRegOperation.write(elem2)).join();
            assertEquals(Sets.newHashSet(subMVReg.read()), Sets.<ByteString>newHashSet(elem2));

            IORMap subORMap = ormap.getORMap(key2);
            subMVReg = subORMap.getMVReg(key1);
            ormap.execute(ORMapOperation.update(key2, key1).with(MVRegOperation.write(elem1))).join();
            assertEquals(Sets.newHashSet(subMVReg.read()), Sets.<ByteString>newHashSet(elem1));
            subMVReg.execute(MVRegOperation.reset()).join();
            assertEquals(Sets.newHashSet(subMVReg.read()), emptySet());
            subMVReg.execute(MVRegOperation.write(elem2)).join();
            assertEquals(Sets.newHashSet(subMVReg.read()), Sets.<ByteString>newHashSet(elem2));
        }

        {
            // sub dwflat
            IDWFlag subDWFlag = ormap.getDWFlag(key1);
            ormap.execute(ORMapOperation.update(key1).with(DWFlagOperation.enable())).join();
            assertTrue(subDWFlag.read());
            subDWFlag.execute(DWFlagOperation.disable()).join();
            assertFalse(subDWFlag.read());
        }

        {
            // sub ewflag
            IEWFlag subEWFlag = ormap.getEWFlag(key1);
            ormap.execute(ORMapOperation.update(key1).with(EWFlagOperation.enable())).join();
            assertTrue(subEWFlag.read());
            subEWFlag.execute(EWFlagOperation.disable()).join();
            assertFalse(subEWFlag.read());
            subEWFlag.execute(EWFlagOperation.enable()).join();
            assertTrue(subEWFlag.read());
        }
    }

    @Test
    public void testJoin() {
        ORMapInflater leftInflater = new ORMapInflater(leftReplica,
            newStateLattice(leftReplica, 1000), executor, Duration.ofMillis(100));
        IORMap leftMap = leftInflater.getCRDT();

        ORMapInflater rightInflater = new ORMapInflater(rightReplica,
            newStateLattice(rightReplica, 1000), executor, Duration.ofMillis(100));
        IORMap rightMap = rightInflater.getCRDT();

        {
            IAWORSet left = leftMap.getAWORSet(key1);
            IAWORSet right = rightMap.getAWORSet(key1);

            left.execute(AWORSetOperation.add(elem1)).join();
            assertTrue(left.contains(elem1));
            right.execute(AWORSetOperation.remove(elem1)).join();
            assertFalse(right.contains(elem1));

            sync(leftInflater, rightInflater);
            assertTrue(left.contains(elem1));
            assertTrue(right.contains(elem1));
        }

        {
            IRWORSet left = leftMap.getRWORSet(key1);
            IRWORSet right = rightMap.getRWORSet(key1);

            left.execute(RWORSetOperation.add(elem1)).join();
            assertTrue(left.contains(elem1));
            right.execute(RWORSetOperation.remove(elem1)).join();
            assertFalse(right.contains(elem1));

            sync(leftInflater, rightInflater);
            assertFalse(left.contains(elem1));
            assertFalse(right.contains(elem1));
        }

        {
            ICCounter left = leftMap.getCCounter(key1);
            ICCounter right = rightMap.getCCounter(key1);

            left.execute(CCounterOperation.add(10)).join();
            assertEquals(left.read(), 10);
            right.execute(CCounterOperation.add(10)).join();
            assertEquals(right.read(), 10);

            sync(leftInflater, rightInflater);
            assertEquals(left.read(), 20);
            assertEquals(right.read(), 20);
        }

        {
            IMVReg left = leftMap.getMVReg(key1);
            IMVReg right = rightMap.getMVReg(key1);

            left.execute(MVRegOperation.write(elem1)).join();
            assertEquals(Sets.newHashSet(left.read()), Sets.<ByteString>newHashSet(elem1));
            right.execute(MVRegOperation.write(elem2)).join();
            assertEquals(Sets.newHashSet(right.read()), Sets.<ByteString>newHashSet(elem2));

            sync(leftInflater, rightInflater);
            assertEquals(Sets.newHashSet(left.read()), Sets.newHashSet(elem1, elem2));
            assertEquals(Sets.newHashSet(right.read()), Sets.newHashSet(elem1, elem2));
        }

        {
            IDWFlag left = leftMap.getDWFlag(key1);
            IDWFlag right = rightMap.getDWFlag(key1);

            left.execute(DWFlagOperation.enable()).join();
            assertTrue(left.read());
            right.execute(DWFlagOperation.disable()).join();
            assertFalse(right.read());

            sync(leftInflater, rightInflater);
            assertFalse(left.read());
            assertFalse(right.read());
        }

        {
            IEWFlag left = leftMap.getEWFlag(key1);
            IEWFlag right = rightMap.getEWFlag(key1);

            left.execute(EWFlagOperation.enable()).join();
            assertTrue(left.read());
            right.execute(EWFlagOperation.disable()).join();
            assertFalse(right.read());

            sync(leftInflater, rightInflater);
            assertTrue(left.read());
            assertTrue(right.read());
        }
    }

    @Test
    public void testJoinAfterCompaction() throws InterruptedException {
        ORMapInflater leftInflater = new ORMapInflater(leftReplica,
            newStateLattice(leftReplica, 1000), executor, Duration.ofMillis(100));
        IORMap leftMap = leftInflater.getCRDT();

        ORMapInflater rightInflater = new ORMapInflater(rightReplica,
            newStateLattice(rightReplica, 100), executor, Duration.ofMillis(100));
        IORMap rightMap = rightInflater.getCRDT();

        leftMap.execute(ORMapOperation.update(key1, key1_1).with(MVRegOperation.write(elem1))).join();
        IMVReg leftMVReg = leftMap.getMVReg(key1, key1_1);
        IMVReg rightMVReg = rightMap.getMVReg(key1, key1_1);

        assertTrue(Sets.newHashSet(leftMVReg.read()).contains(elem1));
        assertFalse(Sets.newHashSet(rightMVReg.read()).contains(elem1));

        sync(leftInflater, rightInflater);
        assertTrue(Sets.newHashSet(leftMVReg.read()).contains(elem1));
        assertTrue(Sets.newHashSet(rightMVReg.read()).contains(elem1));

        rightMap.execute(ORMapOperation.remove(key1, key1_1).of(mvreg)).join();
        assertFalse(Sets.newHashSet(rightMVReg.read()).contains(elem1));

        // wait until rightMap is compacted
        Thread.sleep(1000);

        sync(leftInflater, rightInflater);
        assertTrue(Sets.newHashSet(leftMVReg.read()).contains(elem1));
        assertTrue(Sets.newHashSet(rightMVReg.read()).contains(elem1));
    }

    @Test
    public void testSubCRDTGC() {
        ORMapInflater orMapInflater = new ORMapInflater(leftReplica, newStateLattice(leftReplica, 1000),
            executor, Duration.ofMillis(100));
        IORMap orMap = orMapInflater.getCRDT();

        IORMap subORMap = orMap.getORMap(key1);
        ICCounter subCtr = orMap.getCCounter(key1, key1_1);

        assertEquals(orMap.getORMap(key1), subORMap);
        assertEquals(orMap.getCCounter(key1, key1_1), subCtr);

        int hashCode = subORMap.hashCode();
        subORMap = null;
        System.gc();
        // intermediate ormap is still implicitly referenced by its child CRDT subCtr
        assertEquals(orMap.getORMap(key1).hashCode(), hashCode);

        int subCtrHashCode = subCtr.hashCode();
        subCtr = null;
        System.gc();
        // once subCtr is unreachable, its parent ormap will be gc'ed as well
        assertNotEquals(orMap.getORMap(key1).hashCode(), hashCode);
        assertNotEquals(orMap.getCCounter(key1, key1_1).hashCode(), subCtrHashCode);
    }

    @Test
    public void testInflationSubscriptionWhenGC() {
        ORMapInflater orMapInflater = new ORMapInflater(leftReplica, newStateLattice(leftReplica, 1000),
            executor, Duration.ofMillis(100));
        IORMap orMap = orMapInflater.getCRDT();
        AtomicInteger inflationCount = new AtomicInteger();
        ICCounter subCtr = orMap.getCCounter(key1, key1_1);
        Disposable disposable = subCtr.inflation().subscribe(ts -> inflationCount.incrementAndGet());
        subCtr.execute(CCounterOperation.add(1)).join();
        assertEquals(inflationCount.get(), 1);
        int hashcode = subCtr.hashCode();
        subCtr = null;
        System.gc();
        subCtr = orMap.getCCounter(key1, key1_1);
        assertEquals(subCtr.hashCode(), hashcode);
        subCtr.execute(CCounterOperation.add(1)).join();
        assertEquals(inflationCount.get(), 2);

        subCtr = null;
        disposable.dispose();
        System.gc();
        subCtr = orMap.getCCounter(key1, key1_1);
        assertNotEquals(subCtr.hashCode(), hashcode);
        subCtr.execute(CCounterOperation.add(1)).join();
        assertEquals(inflationCount.get(), 2);
    }
}
