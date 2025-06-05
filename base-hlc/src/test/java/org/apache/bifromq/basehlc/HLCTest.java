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

package org.apache.bifromq.basehlc;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class HLCTest {
    @Test
    public void get() {
        long now = System.currentTimeMillis();
        long t1 = HLC.INST.get();
        assertTrue(t1 > 0 && HLC.INST.getPhysical(t1) >= now);
        for (int i = 0; i < 1000; i++) {
            long t = HLC.INST.get();
            assertTrue(t > t1);
            t1 = t;
        }
    }

    @Test
    public void update() {
        long now = System.currentTimeMillis();
        long t1 = HLC.INST.update(HLC.INST.get());
        assertTrue(t1 > 0 && HLC.INST.getPhysical(t1) >= now);
        for (int i = 0; i < 1000; i++) {
            long t = HLC.INST.update(t1);
            assertTrue(t > t1);
            t1 = t;
        }
    }
}
