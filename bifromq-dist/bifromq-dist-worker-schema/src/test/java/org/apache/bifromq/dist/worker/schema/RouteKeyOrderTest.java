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

package org.apache.bifromq.dist.worker.schema;

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.tenantRouteStartKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.bifromq.util.TopicUtil;
import org.testng.annotations.Test;

public class RouteKeyOrderTest {
    private static final Comparator<ByteString> BYTE_ORDER = ByteString.unsignedLexicographicalComparator();

    @Test
    public void tenantAndLevelOrderMatchUtf8Bytes() {
        List<String> values = new ArrayList<>(List.of("", "a", "t", "zz", "bbb", "中", "éa", "\uE000",
            "\uD800\uDC00", "?", "\uD800", "\uDC00", "a\uD800z", "a".repeat(64), "a".repeat(63) + "b"));
        Random random = new Random(156);
        for (int i = 0; i < 80; i++) {
            StringBuilder value = new StringBuilder();
            for (int j = 0; j < 1 + random.nextInt(8); j++) {
                value.appendCodePoint(1 + random.nextInt(Character.MAX_CODE_POINT));
            }
            values.add(value.toString());
        }
        for (String left : values) {
            for (String right : values) {
                assertEquals(Integer.signum(RouteKeyOrder.TOPIC_LEVEL_ORDER.compare(left, right)),
                    Integer.signum(BYTE_ORDER.compare(tenantRouteStartKey("t", List.of(left)),
                        tenantRouteStartKey("t", List.of(right)))));
                assertEquals(Integer.signum(RouteKeyOrder.TOPIC_LEVEL_ORDER.compare(
                    RouteKeyOrder.tenantIdOrderKey(left), RouteKeyOrder.tenantIdOrderKey(right))),
                    Integer.signum(BYTE_ORDER.compare(tenantBeginKey(left), tenantBeginKey(right))));
            }
        }
    }

    @Test
    public void tenantLengthUsesExistingTwoBytePrefix() {
        for (int length : List.of(127, 128, 255, 256, 32767, 32768, 65535, 65536)) {
            String left = "a".repeat(length);
            String right = "b".repeat(length - 1);
            assertEquals(Integer.signum(RouteKeyOrder.TOPIC_LEVEL_ORDER.compare(
                RouteKeyOrder.tenantIdOrderKey(left), RouteKeyOrder.tenantIdOrderKey(right))),
                Integer.signum(BYTE_ORDER.compare(tenantBeginKey(left), tenantBeginKey(right))));
        }
    }

    @Test
    public void sequencesUseTopicOrderAfterTenant() {
        List<List<String>> filters = List.of(List.of("a"), List.of("a", ""), List.of("a", "", "x"),
            List.of("#"), List.of("+", "x"), List.of("$SYS", "x"), List.of("\uE000"), List.of("\uD800\uDC00"));
        for (List<String> left : filters) {
            for (List<String> right : filters) {
                int expected = Integer.signum(BYTE_ORDER.compare(tenantRouteStartKey("t", left),
                    tenantRouteStartKey("t", right)));
                assertEquals(Integer.signum(RouteKeyOrder.compareLevels(
                    new LinkedList<>(left), new LinkedList<>(right))), expected);
                List<String> globalLeft = new LinkedList<>(left);
                globalLeft.add(0, "t");
                List<String> globalRight = new LinkedList<>(right);
                globalRight.add(0, "t");
                assertEquals(Integer.signum(RouteKeyOrder.compareLevels(
                    RouteKeyOrder.orderedGlobalFilterLevels(globalLeft),
                    RouteKeyOrder.orderedGlobalFilterLevels(globalRight))), expected);
            }
        }
        assertTrue(RouteKeyOrder.compareLevels(List.of("t"), List.of("aa")) > 0);
        assertTrue(RouteKeyOrder.compareLevels(
            List.of(RouteKeyOrder.tenantIdOrderKey("t"), "z"),
            List.of(RouteKeyOrder.tenantIdOrderKey("aa"), "a")) < 0);
    }

    @Test
    public void emptyLevelsDoNotInterleaveRouteSuffixes() {
        var shortFilter = TopicUtil.from("a");
        var longFilter = TopicUtil.from("a//x");
        assertTrue(RouteKeyOrder.compareLevels(shortFilter.getFilterLevelList(),
            longFilter.getFilterLevelList()) < 0);
        assertTrue(BYTE_ORDER.compare(toNormalRouteKey("t", shortFilter, toReceiverUrl(0, "r0", "d")),
            toNormalRouteKey("t", longFilter, toReceiverUrl(0, "r0", "d"))) < 0);
        assertTrue(BYTE_ORDER.compare(toNormalRouteKey("t", shortFilter, toReceiverUrl(0, "r1", "d")),
            toNormalRouteKey("t", longFilter, toReceiverUrl(0, "r1", "d"))) < 0);
    }

    @Test
    public void completeKeysKeepFilterOrderAcrossReceivers() {
        List<String> filters = List.of("a", "a/", "a//x", "a/#", "a/+", "/a", "/", "//", "+", "#",
            "$SYS/x", "\uFF41", "\uD83D\uDE00");
        for (String left : filters) {
            for (String right : filters) {
                if (left.equals(right)) {
                    continue;
                }
                var l = TopicUtil.from(left);
                var r = TopicUtil.from(right);
                int expected = Integer.signum(RouteKeyOrder.compareLevels(
                    l.getFilterLevelList(), r.getFilterLevelList()));
                for (int i = 0; i < 256; i++) {
                    assertEquals(Integer.signum(BYTE_ORDER.compare(
                        toNormalRouteKey("t", l, toReceiverUrl(0, "r" + i, "d")),
                        toNormalRouteKey("t", r, toReceiverUrl(0, "r" + (255 - i), "d")))), expected);
                }
            }
        }
    }
}
