/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basecluster.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RandomUtils {
    public static <T> List<T> uniqueRandomPickAtMost(List<T> list, int max, Predicate<T> predicate) {
        int n = list.size();
        if (n <= max) {
            return list.stream().filter(predicate).collect(Collectors.toList());
        } else {
            HashMap<Integer, Integer> hash = new HashMap<>(2 * max);
            int[] offsets = new int[max];
            for (int i = 0; i < max; i++) {
                int j = i + ThreadLocalRandom.current().nextInt(n - i);
                offsets[i] = (hash.containsKey(j) ? hash.remove(j) : j);
                if (j > i) {
                    hash.put(j, (hash.containsKey(i) ? hash.remove(i) : i));
                }
            }
            List<T> randSelected = new ArrayList<>();
            for (int i : offsets) {
                T peer = list.get(i);
                if (predicate.test(peer)) {
                    randSelected.add(peer);
                }
            }
            return randSelected;
        }
    }
}
