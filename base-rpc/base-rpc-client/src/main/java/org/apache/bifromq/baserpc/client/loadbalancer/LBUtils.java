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

package org.apache.bifromq.baserpc.client.loadbalancer;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class LBUtils {
    private record WeightedValue<X>(Integer weight, X val) {
    }

    static <T> List<T> toWeightedRRSequence(Map<T, Integer> weightedValues) {
        List<WeightedValue<T>> tuples = Lists.newArrayListWithCapacity(weightedValues.size());
        weightedValues.forEach((server, weight) -> tuples.add(new WeightedValue<>(weight, server)));
        List<T> sequence = new ArrayList<>();
        int i = -1;
        int n = tuples.size();
        int currentW = 0;
        List<Integer> weights = tuples.stream().map(tuple -> tuple.weight).collect(Collectors.toList());
        if (!weights.isEmpty()) {
            int maxW = Collections.max(weights);
            int gcdW = getGCD(weights);
            while (true) {
                i = (i + 1) % n;
                if (i == 0) {
                    currentW = currentW - gcdW;
                    if (currentW < 0) {
                        currentW = maxW;
                    }
                    if (currentW == 0) {
                        break;
                    }
                }
                if (weights.get(i) >= currentW) {
                    sequence.add(tuples.get(i).val);
                }
            }
        }
        return sequence;
    }

    private static int gcd(int a, int b) {
        if (a == 0) {
            return b;
        }
        return gcd(b % a, a);
    }

    private static int getGCD(List<Integer> ints) {
        int n = ints.size();
        int result = ints.get(0);
        for (int i = 1; i < n; i++) {
            result = gcd(ints.get(i), result);
        }

        return result;
    }
}
