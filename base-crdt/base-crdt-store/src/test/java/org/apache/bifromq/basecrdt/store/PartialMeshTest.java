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

package org.apache.bifromq.basecrdt.store;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class PartialMeshTest {

    @Test
    public void testNeighbor() {
        NavigableSet<ByteString> mesh = new TreeSet<>(unsignedLexicographicalComparator());
        mesh.add(copyFromUtf8("a"));

        assertTrue(PartialMesh.neighbors(mesh, copyFromUtf8("a")).isEmpty());

        mesh.add(copyFromUtf8("b"));
        Assert.assertEquals(1, PartialMesh.neighbors(mesh, copyFromUtf8("a")).size());
        assertFalse(PartialMesh.neighbors(mesh, copyFromUtf8("a")).contains(copyFromUtf8("a")));

        mesh.add(copyFromUtf8("c"));
        Assert.assertEquals(2, PartialMesh.neighbors(mesh, copyFromUtf8("a")).size());
        assertFalse(PartialMesh.neighbors(mesh, copyFromUtf8("a")).contains(copyFromUtf8("a")));

        mesh.add(copyFromUtf8("d"));
        Assert.assertEquals(2, PartialMesh.neighbors(mesh, copyFromUtf8("a")).size());
        assertFalse(PartialMesh.neighbors(mesh, copyFromUtf8("a")).contains(copyFromUtf8("a")));

        mesh.add(copyFromUtf8("e"));
        Assert.assertEquals(2, PartialMesh.neighbors(mesh, copyFromUtf8("a")).size());
        assertFalse(PartialMesh.neighbors(mesh, copyFromUtf8("a")).contains(copyFromUtf8("a")));

        mesh.add(copyFromUtf8("f"));
        Assert.assertEquals(2, PartialMesh.neighbors(mesh, copyFromUtf8("a")).size());
        assertFalse(PartialMesh.neighbors(mesh, copyFromUtf8("a")).contains(copyFromUtf8("a")));

        mesh.add(copyFromUtf8("g"));
        Assert.assertEquals(2, PartialMesh.neighbors(mesh, copyFromUtf8("a")).size());
        assertFalse(PartialMesh.neighbors(mesh, copyFromUtf8("a")).contains(copyFromUtf8("a")));

        mesh.add(copyFromUtf8("h"));
        Assert.assertEquals(2, PartialMesh.neighbors(mesh, copyFromUtf8("a")).size());
        assertFalse(PartialMesh.neighbors(mesh, copyFromUtf8("a")).contains(copyFromUtf8("a")));

        mesh.add(copyFromUtf8("i"));
        Assert.assertEquals(2, PartialMesh.neighbors(mesh, copyFromUtf8("a")).size());
        assertFalse(PartialMesh.neighbors(mesh, copyFromUtf8("a")).contains(copyFromUtf8("a")));

        for (int i = 0; i < 20; ++i) {
            mesh.add(copyFromUtf8(String.valueOf(i)));
        }
        Assert.assertEquals(4, PartialMesh.neighbors(mesh, copyFromUtf8("a")).size());
    }

    @Test
    public void testNeighborWithIgnore() {
        NavigableSet<ByteString> mesh = new TreeSet<>(unsignedLexicographicalComparator());
        mesh.add(copyFromUtf8("a"));
        mesh.add(copyFromUtf8("b"));

        Set<ByteString> ignores = new HashSet<>();
        ignores.add(copyFromUtf8("b"));

        assertTrue(PartialMesh.neighbors(mesh, ignores, copyFromUtf8("a")).isEmpty());

        mesh.add(copyFromUtf8("c"));
        Assert.assertEquals(1, PartialMesh.neighbors(mesh, ignores, copyFromUtf8("a")).size());
        assertTrue(PartialMesh.neighbors(mesh, ignores, copyFromUtf8("a")).contains(copyFromUtf8("c")));

        mesh.add(copyFromUtf8("d"));
        Assert.assertEquals(2, PartialMesh.neighbors(mesh, ignores, copyFromUtf8("a")).size());
        assertTrue(PartialMesh.neighbors(mesh, ignores, copyFromUtf8("a")).contains(copyFromUtf8("c")));
        assertTrue(PartialMesh.neighbors(mesh, ignores, copyFromUtf8("a")).contains(copyFromUtf8("d")));

        ignores.add(copyFromUtf8("c"));
        Assert.assertEquals(1, PartialMesh.neighbors(mesh, ignores, copyFromUtf8("a")).size());
        assertTrue(PartialMesh.neighbors(mesh, ignores, copyFromUtf8("a")).contains(copyFromUtf8("d")));

        mesh.clear();
        ignores.clear();
        for (char i = 'a'; i <= 'z'; ++i) {
            mesh.add(copyFromUtf8(String.valueOf(i)));
        }
        ignores.add(copyFromUtf8("b"));
        Set<ByteString> neighbors = PartialMesh.neighbors(mesh, ignores, copyFromUtf8("a"));
        Assert.assertEquals(4, neighbors.size());
        assertTrue(neighbors.contains(copyFromUtf8("c")));
        assertTrue(neighbors.contains(copyFromUtf8("d")));
        assertTrue(neighbors.contains(copyFromUtf8("z")));
        assertTrue(neighbors.contains(copyFromUtf8("y")));
    }

    @Test
    public void demoNeighborsInDifferentScale() {
        for (int i = 1; i <= 1024; i++) {
            demoNeighborsInDifferentScale(i);
        }
    }

    private void demoNeighborsInDifferentScale(int s) {
        NavigableSet<ByteString> mesh = new TreeSet<>(unsignedLexicographicalComparator());
        for (int i = 0; i < s; i++) {
            mesh.add(copyFromUtf8(Integer.toString(i)));
        }
        log.debug("Scale:{}, Neighbors:{}", s, PartialMesh.neighbors(mesh, copyFromUtf8(Integer.toString(1))).size());
    }

    @Test
    public void demoBroadcastPerformance() {
        // this case demonstrates how fast partial mesh network topology convergent in various scales
        for (int i = 1; i <= 128; i++) {
            log.debug("completely broadcast {} nodes network, in {} rounds", i, simulateColoring(i));
        }
        // here are some additional testing result
        // completely broadcast 2048 nodes network, in 6 rounds(21 neighbors)
        // completely broadcast 4096 nodes network, in 6 rounds(23 neighbors)
        // completely broadcast 8192 nodes network, in 7 rounds(25 neighbors)
        // completely broadcast 102400 nodes network, in 8 rounds(34 neighbors)
    }

    private int simulateColoring(int scale) {
        boolean[] keys = new boolean[scale];
        TreeSet<Integer> unvisited = new TreeSet<>();
        TreeSet<Integer> visited = new TreeSet<>();
        TreeSet<ByteString> mesh = new TreeSet<>(unsignedLexicographicalComparator());
        for (int i = 0; i < scale; i++) {
            keys[i] = false;
            unvisited.add(i);
            mesh.add(copyFromUtf8(Integer.toString(i)));
        }
        int round = 0;
        keys[0] = true;
        int t = 1;
        int neighborSize = PartialMesh.neighbors(mesh, copyFromUtf8(Integer.toString(0))).size();
        do {
            round++;
            Set<Integer> coloredInRound = Sets.newHashSet();
            for (int i : unvisited) {
                if (keys[i] && !visited.contains(i)) {
                    if (keys[i] && !coloredInRound.contains(i)) {
                        Set<ByteString> n = PartialMesh.neighbors(mesh, copyFromUtf8(Integer.toString(i)));
                        int c = 0;
                        for (ByteString key : n) {
                            int k = Integer.parseInt(key.toStringUtf8());
                            if (!keys[k]) {
                                keys[k] = true;
                                t++;
                                c++;
                                coloredInRound.add(k);
                            }
                        }
//                        if (c > 0) {
//                            log.info("process: {}, ratio: {}", j, (float) c / n.size());
//                        }
                        visited.add(i);
                    }
                }
            }
            unvisited.removeAll(visited);
            log.debug("round {}, neighbors:{}, unvisited:{}, visited:{} ratio {}",
                round, neighborSize, unvisited.size(), visited.size(), (float) t / keys.length);
        } while (t != scale);
        return round;
    }
}
