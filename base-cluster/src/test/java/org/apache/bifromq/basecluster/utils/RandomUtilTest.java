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

package org.apache.bifromq.basecluster.utils;

import static java.util.Collections.emptyList;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecluster.util.RandomUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RandomUtilTest {
    @Test
    public void pickFromEmpty() {
        assertTrue(RandomUtils.uniqueRandomPickAtMost(emptyList(), 0, i -> true).isEmpty());
        assertTrue(RandomUtils.uniqueRandomPickAtMost(emptyList(), 5, i -> true).isEmpty());
    }

    @Test
    public void pickNothing() {
        assertTrue(RandomUtils.uniqueRandomPickAtMost(generateList(10), 0, i -> true).isEmpty());
    }

    @Test
    public void pickAll() {
        List<Integer> orig = generateList(10);
        Assert.assertEquals(orig, RandomUtils.uniqueRandomPickAtMost(orig, 10, i -> true));
        Assert.assertEquals(orig, RandomUtils.uniqueRandomPickAtMost(orig, 20, i -> true));
    }

    @Test
    public void pickMatch() {
        List<Integer> orig = generateList(10);
        Assert.assertEquals(5, RandomUtils.uniqueRandomPickAtMost(orig, 10, i -> i % 2 == 0).size());
        assertTrue(RandomUtils.uniqueRandomPickAtMost(orig, 3, i -> i % 2 == 0).size() <= 3);
    }

    private List<Integer> generateList(int size) {
        List<Integer> intList = new ArrayList<>(size);
        IntStream.range(0, size).forEach(intList::add);
        Collections.shuffle(intList);
        return intList;
    }
}
