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

package org.apache.bifromq.basecrdt.core.internal;

import org.apache.bifromq.basecrdt.proto.Replacement;
import org.apache.bifromq.basecrdt.proto.StateLattice;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

interface IReplicaStateLattice {
    interface JoinDiff {
        Iterable<StateLattice> adds();

        Iterable<StateLattice> removes();
    }

    int size();

    Duration historyDuration();

    /**
     * The next event which could be used to construct state lattice.
     *
     * @return the version for next event
     */
    long nextEvent();

    /**
     * The state-addition events contributed by the provided replica.
     *
     * @return
     */
    Iterator<StateLattice> lattices();


    /**
     * Join a lattice into current state and return the essential diff made to the previous state, and memoize removed
     * events until exceed history duration.
     *
     * @param state the lattice to join
     * @return
     */
    JoinDiff join(Iterable<Replacement> state);

    /**
     * Get a lattice containing only the delta part by comparing the provided index with local index. The third argument
     * controls the maximum events included in the delta
     *
     * @param coveredLatticeIndex the index of covered lattices to exclude from the delta
     * @param coveredHistoryIndex the index of covered history to exclude from the delta
     * @param maxEvents           the maximum events a state fragment could contain
     * @return if no delta found returns Optional.empty()
     */
    Optional<Iterable<Replacement>> delta(
        Map<ByteString, NavigableMap<Long, Long>> coveredLatticeIndex,
        Map<ByteString, NavigableMap<Long, Long>> coveredHistoryIndex,
        int maxEvents);

    /**
     * Get the summarized history of events contributing to current state.
     *
     * @return
     */
    Map<ByteString, NavigableMap<Long, Long>> latticeIndex();

    /**
     * Get the summarized history of events which were contributing to local state.
     *
     * @return
     */
    Map<ByteString, NavigableMap<Long, Long>> historyIndex();

    /**
     * Compact to free some memory occupied by obsolete history.
     *
     * @return if next compaction should be scheduled
     */
    boolean compact();
}
