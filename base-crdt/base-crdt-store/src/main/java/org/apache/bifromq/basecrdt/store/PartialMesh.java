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

package org.apache.bifromq.basecrdt.store;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public final class PartialMesh {
    private static class Ring {
        private final ArrayList<ByteString> ring;

        Ring(SortedSet keys) {
            ring = new ArrayList<>(keys);
        }

        int indexOf(ByteString key) {
            return ring.indexOf(key);
        }

        ByteString get(int i) {
            int idx = (i % ring.size() + ring.size()) % ring.size();
            return ring.get(idx);
        }
    }

    public static Set<ByteString> neighbors(SortedSet<ByteString> cluster, ByteString key) {
        return neighbors(cluster, Collections.emptySet(), key);
    }

    public static Set<ByteString> neighbors(SortedSet<ByteString> cluster, Set<ByteString> ignore, ByteString key) {
        Ring ring = new Ring(cluster);
        Set<ByteString> n = new TreeSet<>(ByteString.unsignedLexicographicalComparator());
        assert !ignore.contains(key);
        if (cluster.contains(key)) {
            int index = ring.indexOf(key);
            int offset = 1;
            while (ignore.contains(ring.get(index + offset))) {
                ++offset;
            }
            n.add(ring.get(index + offset));
            offset = 1;
            while (ignore.contains(ring.get(index - offset))) {
                ++offset;
            }
            n.add(ring.get(index - offset));

            // add more adjacent keys based the height of complete binary tree
            int step = step(cluster.size());
            for (int i = 1; i <= step; i++) {
                offset = 2 << (i - 1);
                while (ignore.contains(ring.get(index + offset)) || n.contains(ring.get(index + offset))) {
                    ++offset;
                    if (ring.get(index + offset) == key) {
                        break;
                    }
                }
                n.add(ring.get(index + offset));

                offset = 2 << (i - 1);
                while (ignore.contains(ring.get(index - offset)) || n.contains(ring.get(index - offset))) {
                    ++offset;
                    if (ring.get(index - offset) == key) {
                        break;
                    }
                }
                n.add(ring.get(index - offset));
            }
        }
        n.remove(key);
        return n;
    }

    static int step(int size) {
        return (int) (Math.log10(size) / Math.log10(20));
    }
}
