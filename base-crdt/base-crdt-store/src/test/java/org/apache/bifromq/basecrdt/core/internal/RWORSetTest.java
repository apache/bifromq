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
import static org.apache.bifromq.basecrdt.core.api.CausalCRDTType.rworset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.time.Duration;
import org.apache.bifromq.basecrdt.core.api.IRWORSet;
import org.apache.bifromq.basecrdt.core.api.RWORSetOperation;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.testng.annotations.Test;

public class RWORSetTest extends CRDTTest {
    private final Replica leftReplica = Replica.newBuilder()
        .setUri(toURI(rworset, "rworset"))
        .setId(ByteString.copyFromUtf8("left-address"))
        .build();
    private final Replica rightReplica = Replica.newBuilder()
        .setUri(toURI(rworset, "rworset"))
        .setId(ByteString.copyFromUtf8("right-address"))
        .build();
    private final ByteString elem1 = ByteString.copyFromUtf8("e1");
    private final ByteString elem2 = ByteString.copyFromUtf8("e2");
    private final ByteString elem3 = ByteString.copyFromUtf8("e3");

    @Test
    public void testOperation() {
        RWORSetInflater rworSetInflater =
            new RWORSetInflater("leftStore", leftReplica, newStateLattice(leftReplica, 1000),
                executor, Duration.ofMillis(100));
        IRWORSet rworSet = rworSetInflater.getCRDT();
        assertEquals(rworSet.id(), leftReplica);

        assertTrue(rworSet.isEmpty());
        assertFalse(rworSet.elements().hasNext());

        rworSet.execute(RWORSetOperation.add(elem1)).join();
        assertFalse(rworSet.isEmpty());
        assertTrue(rworSet.contains(elem1));
        TestUtil.assertSame(Sets.<ByteString>newHashSet(elem1).iterator(), rworSet.elements());

        rworSet.execute(RWORSetOperation.remove(elem1)).join();
        assertTrue(rworSet.isEmpty());
        assertFalse(rworSet.contains(elem1));
        assertFalse(rworSet.elements().hasNext());

        rworSet.execute(RWORSetOperation.add(elem1));
        rworSet.execute(RWORSetOperation.add(elem2));
        rworSet.execute(RWORSetOperation.add(elem3)).join();
        assertTrue(rworSet.contains(elem1));
        assertTrue(rworSet.contains(elem2));
        assertTrue(rworSet.contains(elem3));

        rworSet.execute(RWORSetOperation.clear());
        rworSet.execute(RWORSetOperation.add(elem1)).join();
        assertTrue(rworSet.contains(elem1));
        TestUtil.assertSame(Sets.<ByteString>newHashSet(elem1).iterator(), rworSet.elements());
    }

    @Test
    public void testJoin() {
        RWORSetInflater leftInflater = new RWORSetInflater("leftStore", leftReplica,
            newStateLattice(leftReplica, 1000), executor, Duration.ofMillis(100));
        IRWORSet left = leftInflater.getCRDT();

        RWORSetInflater rightInflater = new RWORSetInflater("rightStore", rightReplica,
            newStateLattice(rightReplica, 1000), executor, Duration.ofMillis(100));
        IRWORSet right = rightInflater.getCRDT();

        left.execute(RWORSetOperation.add(elem1));
        left.execute(RWORSetOperation.add(elem2));
        left.execute(RWORSetOperation.add(elem3)).join();

        sync(leftInflater, rightInflater);
        assertTrue(right.contains(elem1));
        assertTrue(right.contains(elem2));
        assertTrue(right.contains(elem3));

        // remove win
        left.execute(RWORSetOperation.add(elem1)).join();
        right.execute(RWORSetOperation.remove(elem1)).join();
        sync(rightInflater, leftInflater);
        assertFalse(right.contains(elem1));
    }
}
