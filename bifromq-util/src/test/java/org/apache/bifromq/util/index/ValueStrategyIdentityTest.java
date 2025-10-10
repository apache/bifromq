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

package org.apache.bifromq.util.index;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;

public class ValueStrategyIdentityTest {

    @Test
    public void testIdentityStrategyOnValueRef() {
        ValueStrategy<Box> s = ValueStrategy.identity();
        Box a1 = new Box("a");
        Box a2 = new Box("a");

        ValueRef<Box> r1 = ValueRef.of(a1, s);
        ValueRef<Box> r2 = ValueRef.of(a1, s);
        ValueRef<Box> r3 = ValueRef.of(a2, s);

        assertEquals(r2, r1);
        assertNotEquals(r3, r1);
    }
}

