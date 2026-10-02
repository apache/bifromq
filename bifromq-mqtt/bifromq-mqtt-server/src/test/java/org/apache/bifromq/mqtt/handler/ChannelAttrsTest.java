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

package org.apache.bifromq.mqtt.handler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class ChannelAttrsTest {

    private static int varintLength(int length) {
        if (length <= 127) {
            return 1;
        }
        if (length <= 16383) {
            return 2;
        }
        if (length <= 2097151) {
            return 3;
        }
        return 4;
    }

    private static int totalPacketSize(int remainingLength) {
        return 1 + varintLength(remainingLength) + remainingLength;
    }

    @Test
    public void testMaxRemainingLengthBoundaries() {
        assertEquals(ChannelAttrs.maxRemainingLength(0), 0);
        assertEquals(ChannelAttrs.maxRemainingLength(1), 0);
        assertEquals(ChannelAttrs.maxRemainingLength(2), 0);

        assertEquals(ChannelAttrs.maxRemainingLength(3), 1);
        assertEquals(ChannelAttrs.maxRemainingLength(129), 127);

        assertEquals(ChannelAttrs.maxRemainingLength(130), 127);
        assertEquals(ChannelAttrs.maxRemainingLength(131), 128);
        assertEquals(ChannelAttrs.maxRemainingLength(16384), 16381);
        assertEquals(ChannelAttrs.maxRemainingLength(16386), 16383);

        assertEquals(ChannelAttrs.maxRemainingLength(16387), 16383);
        assertEquals(ChannelAttrs.maxRemainingLength(16388), 16384);
        assertEquals(ChannelAttrs.maxRemainingLength(2097155), 2097151);

        assertEquals(ChannelAttrs.maxRemainingLength(2097156), 2097151);
        assertEquals(ChannelAttrs.maxRemainingLength(2097157), 2097152);
    }

    @Test
    public void testMaxRemainingLengthExactFit() {
        int[] testLimits = {
            2, 3, 4, 10, 127, 128, 129, 130, 131, 1000, 16383, 16384, 16385, 16386, 16387, 16388,
            65535, 65536, 100000, 2097154, 2097155, 2097156, 2097157, 10000000
        };

        for (int limit : testLimits) {
            int maxR = ChannelAttrs.maxRemainingLength(limit);
            if (limit <= 2) {
                assertEquals(maxR, 0);
            } else {
                assertTrue(totalPacketSize(maxR) <= limit,
                    "Total packet size for maxR=" + maxR + " should be <= limit=" + limit);
                assertTrue(totalPacketSize(maxR + 1) > limit,
                    "Total packet size for (maxR+1)=" + (maxR + 1) + " should exceed limit=" + limit);
            }
        }
    }
}
