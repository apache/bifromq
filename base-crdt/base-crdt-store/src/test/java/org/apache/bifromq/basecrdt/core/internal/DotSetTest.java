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

import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.dot;
import static org.apache.bifromq.basecrdt.core.internal.ProtoUtils.singleDot;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

public class DotSetTest {
    private final ByteString replicaA = copyFromUtf8("A");
    private final ByteString replicaB = copyFromUtf8("B");

    @Test
    public void testAdd() {
        DotSet dotSet = new DotSet();
        assertTrue(dotSet.isBottom());
        assertFalse(dotSet.iterator().hasNext());

        assertTrue(dotSet.add(singleDot(replicaA, 1)));
        assertFalse(dotSet.add(singleDot(replicaA, 1)));
        assertTrue(dotSet.add(singleDot(replicaB, 1)));
        assertFalse(dotSet.isBottom());

        TestUtil.assertUnorderedSame(Sets.newHashSet(dot(replicaA, 1), dot(replicaB, 1)).iterator(), dotSet.iterator());
    }

    @Test
    public void testRemove() {
        DotSet dotSet = new DotSet();

        dotSet.add(singleDot(replicaA, 1));
        dotSet.add(singleDot(replicaB, 1));

        assertTrue(dotSet.remove(singleDot(replicaA, 1)));
        assertFalse(dotSet.remove(singleDot(replicaA, 1)));
        assertTrue(dotSet.remove(singleDot(replicaB, 1)));
        assertTrue(dotSet.isBottom());
        assertFalse(dotSet.iterator().hasNext());
    }
}
