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
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.aworset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.time.Duration;
import org.apache.bifromq.basecrdt.core.api.AWORSetOperation;
import org.apache.bifromq.basecrdt.core.api.IAWORSet;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.testng.annotations.Test;

public class AWORSetTest extends CRDTTest {
    private final Replica leftReplica = Replica.newBuilder()
        .setUri(toURI(aworset, "aworset"))
        .setId(ByteString.copyFromUtf8("left-address"))
        .build();
    private final Replica rightReplica = Replica.newBuilder()
        .setUri(toURI(aworset, "aworset"))
        .setId(ByteString.copyFromUtf8("right-address"))
        .build();
    private final ByteString elem1 = ByteString.copyFromUtf8("e1");
    private final ByteString elem2 = ByteString.copyFromUtf8("e2");
    private final ByteString elem3 = ByteString.copyFromUtf8("e3");

    @Test
    public void testOperation() {
        AWORSetInflater aworSetInflater = new AWORSetInflater("testStore", leftReplica,
            newStateLattice(leftReplica, 1000),
            executor, Duration.ofMillis(100));
        IAWORSet aworSet = aworSetInflater.getCRDT();
        assertEquals(aworSet.id(), leftReplica);

        assertTrue(aworSet.isEmpty());
        assertFalse(aworSet.elements().hasNext());

        aworSet.execute(AWORSetOperation.add(elem1)).join();
        assertFalse(aworSet.isEmpty());
        assertTrue(aworSet.contains(elem1));
        TestUtil.assertSame(Sets.<ByteString>newHashSet(elem1).iterator(), aworSet.elements());

        aworSet.execute(AWORSetOperation.remove(elem1)).join();
        assertTrue(aworSet.isEmpty());
        assertFalse(aworSet.contains(elem1));
        assertFalse(aworSet.elements().hasNext());

        aworSet.execute(AWORSetOperation.add(elem1));
        aworSet.execute(AWORSetOperation.add(elem2));
        aworSet.execute(AWORSetOperation.add(elem3)).join();
        assertTrue(aworSet.contains(elem1));
        assertTrue(aworSet.contains(elem2));
        assertTrue(aworSet.contains(elem3));

        aworSet.execute(AWORSetOperation.clear());
        aworSet.execute(AWORSetOperation.add(elem1)).join();
        assertTrue(aworSet.contains(elem1));
        TestUtil.assertSame(Sets.<ByteString>newHashSet(elem1).iterator(), aworSet.elements());
    }

    @Test
    public void testJoin() {
        AWORSetInflater leftInflater = new AWORSetInflater("leftStore", leftReplica,
            newStateLattice(leftReplica, 1000), executor, Duration.ofMillis(100));
        IAWORSet left = leftInflater.getCRDT();

        AWORSetInflater rightInflater = new AWORSetInflater("rightStore", rightReplica,
            newStateLattice(rightReplica, 1000), executor, Duration.ofMillis(100));
        IAWORSet right = rightInflater.getCRDT();

        left.execute(AWORSetOperation.add(elem1));
        left.execute(AWORSetOperation.add(elem2));
        left.execute(AWORSetOperation.add(elem3)).join();

        sync(leftInflater, rightInflater);
        assertTrue(right.contains(elem1));
        assertTrue(right.contains(elem2));
        assertTrue(right.contains(elem3));

        // add win
        left.execute(AWORSetOperation.add(elem1)).join();
        right.execute(AWORSetOperation.remove(elem1)).join();
        sync(rightInflater, leftInflater);
        assertTrue(right.contains(elem1));

        // clear
        left.execute(AWORSetOperation.clear()).join();
        sync(rightInflater, leftInflater);
        assertTrue(left.isEmpty());
        assertTrue(right.isEmpty());
    }
}
