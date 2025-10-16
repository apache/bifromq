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

package org.apache.bifromq.basekv.raft;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import org.apache.bifromq.basekv.raft.proto.Voting;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Base class for state store test cases.
 */
public abstract class BasicStateStoreTest {

    protected abstract IRaftStateStore createStorage(String id, Snapshot initSnapshot);

    protected abstract String localId();

    @Test
    public void testInit() {
        Snapshot initSnapshot = Snapshot.newBuilder()
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters(localId())
                .addVoters(localId() + "_2")
                .addVoters(localId() + "_3")
                .build())
            .setIndex(0)
            .setTerm(0)
            .build();
        IRaftStateStore stateStorage = createStorage(localId(), initSnapshot);
        assertEquals(stateStorage.local(), localId());
        assertEquals(stateStorage.firstIndex(), 1);
        assertEquals(stateStorage.lastIndex(), 0);
        assertEquals(stateStorage.latestSnapshot(), initSnapshot);
        assertEquals(stateStorage.latestClusterConfig(), initSnapshot.getClusterConfig());
        Assert.assertFalse(stateStorage.entryAt(1).isPresent());
        Assert.assertFalse(stateStorage.entryAt(0).isPresent());
        try {
            stateStorage.entries(0, 1, -1);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
    }

    @Test
    public void testSaveAndLoadCurrentTerm() {
        IRaftStateStore stateStorage = setupStateStorage();
        assertEquals(stateStorage.currentTerm(), 0);
        stateStorage.saveTerm(0);
        assertEquals(stateStorage.currentTerm(), 0);
        stateStorage.saveTerm(1);
        assertEquals(stateStorage.currentTerm(), 1);
    }

    @Test
    public void testSaveAndLoadVoting() {
        IRaftStateStore stateStorage = setupStateStorage();
        Assert.assertFalse(stateStorage.currentVoting().isPresent());
        Voting voting = Voting.newBuilder().setFor(localId()).setTerm(1L).build();
        stateStorage.saveVoting(voting);
        assertEquals(stateStorage.currentVoting().get(), voting);
    }

    @Test
    public void testSyncAppend() {
        IRaftStateStore stateStorage = setupStateStorage();
        List<Long> stabledIndexes = new ArrayList<>();
        stateStorage.addStableListener(stabledIndexes::add);
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.EMPTY)
                .build();
            stateStorage.append(singletonList(entry), true);
            assertEquals(stateStorage.entryAt(stateStorage.lastIndex()).get().getIndex(), stateStorage.lastIndex());
        }
        for (int i = 0; i < stabledIndexes.size(); i++) {
            assertEquals(stabledIndexes.get(i), Long.valueOf(i + 1));
        }
    }

    @Test
    public void testAppendFirstEntryFailedWithLowerBoundCheck() {
        ClusterConfig initialClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .addVoters(localId() + "_2")
            .addVoters(localId() + "_3")
            .build();

        Snapshot initSnapshot = Snapshot.newBuilder()
            .setClusterConfig(initialClusterConfig)
            .setIndex(5)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = createStorage(localId(), initSnapshot);
        try {
            stateStorage.append(List.of(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(5)
                .setData(ByteString.EMPTY)
                .build()), true);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
        try {
            stateStorage.append(List.of(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(7)
                .setData(ByteString.EMPTY)
                .build()), true);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
        stateStorage.append(asList(
            LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(6)
                .setData(ByteString.EMPTY)
                .build(),
            LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(7)
                .setData(ByteString.EMPTY)
                .build()), true);

        try {
            stateStorage.append(List.of(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(5)
                .setData(ByteString.EMPTY)
                .build()), true);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
        try {
            stateStorage.append(List.of(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(9)
                .setData(ByteString.EMPTY)
                .build()), true);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IndexOutOfBoundsException);
        }
    }

    @Test
    public void testAsyncAppend() throws InterruptedException {
        IRaftStateStore stateStorage = setupStateStorage();
        List<Long> stabledIndexes = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        stateStorage.addStableListener((stableIndex) -> {
            stabledIndexes.add(stableIndex);
            countDownLatch.countDown();
        });
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.EMPTY)
                .build();
            stateStorage.append(singletonList(entry), false);
            assertEquals(stateStorage.entryAt(stateStorage.lastIndex()).get().getIndex(), stateStorage.lastIndex());
        }
        countDownLatch.await();
        assertFalse(stabledIndexes.isEmpty());
        assertTrue(stabledIndexes.get(0) >= 1);
    }

    @Test
    public void testTruncateAndAppend() {
        IRaftStateStore stateStorage = setupStateStorage();
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(updatedClusterConfig);
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            LogEntry entry = entryBuilder.build();
            stateStorage.append(singletonList(entry), true);
            assertEquals(stateStorage.lastIndex(), entry.getIndex());
        }
        count = 5;
        List<LogEntry> newEntries = new ArrayList<>();
        while (count <= 8) {
            // prepare entries of [5,8]
            newEntries.add(LogEntry.newBuilder()
                .setTerm(2)
                .setIndex(count)
                .setData(ByteString.copyFromUtf8("Data:" + count))
                .build());
            count++;
        }
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        stateStorage.append(newEntries, true);
        assertNotEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        assertEquals(stateStorage.firstIndex(), 1);
        assertEquals(stateStorage.lastIndex(), 8);
        try (ILogEntryIterator entries5To8 = stateStorage.entries(5, 9, -1)) {
            for (int i = 0; entries5To8.hasNext(); i++) {
                LogEntry entry = entries5To8.next();
                assertEquals(entry.getTerm(), 2);
                assertEquals(entry.getIndex(), i + 5);
            }
        }
    }

    @Test
    public void testTruncateWholeLog() {
        ClusterConfig initialClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .addVoters(localId() + "_2")
            .addVoters(localId() + "_3")
            .build();

        Snapshot initSnapshot = Snapshot.newBuilder()
            .setClusterConfig(initialClusterConfig)
            .setIndex(5)
            .setTerm(1)
            .build();
        IRaftStateStore stateStorage = createStorage(localId(), initSnapshot);
        // log starts from 6
        assertEquals(stateStorage.firstIndex(), 6);
        assertEquals(stateStorage.lastIndex(), 5);

        List<LogEntry> newEntries = List.of(LogEntry.newBuilder()
            .setTerm(1)
            .setIndex(6)
            .setData(ByteString.EMPTY)
            .build());
        stateStorage.append(newEntries, true);
        assertEquals(stateStorage.firstIndex(), 6);
        assertEquals(stateStorage.lastIndex(), 6);

        newEntries = asList(LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(6)
                .setData(ByteString.EMPTY)
                .build(),
            LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(7)
                .setData(ByteString.EMPTY)
                .build());
        stateStorage.append(newEntries, true);
        assertEquals(stateStorage.firstIndex(), 6);
        assertEquals(stateStorage.lastIndex(), 7);

        newEntries = List.of(LogEntry.newBuilder()
            .setTerm(1)
            .setIndex(6)
            .setData(ByteString.EMPTY)
            .build());
        stateStorage.append(newEntries, true);
        assertEquals(stateStorage.firstIndex(), 6);
        assertEquals(stateStorage.lastIndex(), 6);
    }

    @Test
    public void testTruncateAndAppendFromZeroIndex() {
        IRaftStateStore stateStorage = setupStateStorage();
        List<Long> stabledIndexes = new ArrayList<>();
        stateStorage.addStableListener(stabledIndexes::add);
        int count = 5;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.EMPTY)
                .build();
            stateStorage.append(singletonList(entry), true);
        }

        int i = 1;
        List<LogEntry> newEntries = new ArrayList<>();
        while (i <= 5) {
            // prepare entries of [5,8]
            newEntries.add(LogEntry.newBuilder()
                .setTerm(2)
                .setIndex(i)
                .setData(ByteString.copyFromUtf8("Data:" + count))
                .build());
            i++;
        }
        stateStorage.append(newEntries, true);
        assertEquals(stateStorage.firstIndex(), 1);
        assertEquals(stateStorage.lastIndex(), 5);
        assertEquals(stateStorage.entryAt(stateStorage.firstIndex()).get().getTerm(), 2);
        assertEquals(stateStorage.entryAt(stateStorage.lastIndex()).get().getTerm(), 2);
    }

    @Test
    public void testApplyPartialSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(updatedClusterConfig);
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            stateStorage.append(singletonList(entryBuilder.build()), true);
        }
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(stateStorage.latestClusterConfig())
            .setData(ByteString.EMPTY)
            .setTerm(1)
            .setIndex(3)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        assertEquals(stateStorage.firstIndex(), 4);
        assertEquals(stateStorage.lastIndex(), 10);
    }

    @Test
    public void testOverrideWithSameSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(updatedClusterConfig);
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            stateStorage.append(singletonList(entryBuilder.build()), true);
        }
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(stateStorage.latestClusterConfig())
            .setData(ByteString.EMPTY)
            .setTerm(1)
            .setIndex(3)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        assertEquals(stateStorage.firstIndex(), 4);
        assertEquals(stateStorage.lastIndex(), 10);
        assertEquals(stateStorage.latestSnapshot(), snapshot);

        snapshot = Snapshot.newBuilder()
            .setClusterConfig(stateStorage.latestClusterConfig())
            .setData(ByteString.copyFromUtf8("hello"))
            .setTerm(1)
            .setIndex(3)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        assertEquals(stateStorage.firstIndex(), 4);
        assertEquals(stateStorage.lastIndex(), 10);
        assertEquals(stateStorage.latestSnapshot(), snapshot);
    }

    @Test
    public void testOverrideSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(updatedClusterConfig);
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            stateStorage.append(singletonList(entryBuilder.build()), true);
        }
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(stateStorage.latestClusterConfig())
            .setData(ByteString.EMPTY)
            .setTerm(1)
            .setIndex(3)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        assertEquals(stateStorage.firstIndex(), 4);
        assertEquals(stateStorage.lastIndex(), 10);
        assertEquals(stateStorage.latestSnapshot(), snapshot);

        snapshot = Snapshot.newBuilder()
            .setClusterConfig(stateStorage.latestClusterConfig())
            .setData(ByteString.copyFromUtf8("hello"))
            .setTerm(2)
            .setIndex(3)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        assertEquals(stateStorage.firstIndex(), 4);
        assertEquals(stateStorage.lastIndex(), 3);
        assertEquals(stateStorage.latestSnapshot(), snapshot);
    }

    @Test
    public void testApplyFullSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(updatedClusterConfig);
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            stateStorage.append(singletonList(entryBuilder.build()), true);
        }
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(stateStorage.latestClusterConfig())
            .setData(ByteString.EMPTY)
            .setTerm(1)
            .setIndex(10)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        assertEquals(stateStorage.firstIndex(), 11);
        assertEquals(stateStorage.lastIndex(), 10);
        assertEquals(stateStorage.latestSnapshot(), snapshot);
    }

    @Test
    public void testApplyLargerSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)))
                .build();
            stateStorage.append(singletonList(entry), true);
        }
        ClusterConfig updatedClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .build();
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(updatedClusterConfig)
            .setData(ByteString.EMPTY)
            .setTerm(1)
            .setIndex(15)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(stateStorage.latestClusterConfig(), updatedClusterConfig);
        assertEquals(stateStorage.firstIndex(), 16);
        assertEquals(stateStorage.lastIndex(), 15);
        assertEquals(stateStorage.latestSnapshot(), snapshot);
    }

    @Test
    public void testApplyDifferentSnapshot() {
        IRaftStateStore stateStorage = setupStateStorage();
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)))
                .build();
            stateStorage.append(singletonList(entry), true);
        }
        ClusterConfig cc = ClusterConfig.newBuilder().addVoters("V1").build();
        Snapshot snapshot = Snapshot.newBuilder()
            .setClusterConfig(cc)
            .setData(ByteString.EMPTY)
            .setTerm(2) // wrong term
            .setIndex(3)
            .build();
        stateStorage.applySnapshot(snapshot);
        assertEquals(stateStorage.lastIndex(), 3);
        assertEquals(stateStorage.firstIndex(), 4);
        assertEquals(stateStorage.latestSnapshot().getIndex(), 3);
        assertEquals(stateStorage.latestSnapshot().getTerm(), 2);
        assertEquals(stateStorage.latestClusterConfig(), cc);
        assertEquals(stateStorage.latestSnapshot(), snapshot);
    }

    @Test
    public void testFetchEntries() {
        IRaftStateStore stateStorage = setupStateStorage();
        try (ILogEntryIterator entries = stateStorage.entries(stateStorage.firstIndex(), stateStorage.lastIndex(), -1)) {
            assertFalse(entries.hasNext());
        }
        int count = 10;
        while (count-- > 0) {
            LogEntry entry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(stateStorage.lastIndex() + 1)
                .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)))
                .build();
            stateStorage.append(singletonList(entry), true);
        }
        AtomicInteger counter = new AtomicInteger(0);
        try (ILogEntryIterator it = stateStorage.entries(stateStorage.firstIndex(), stateStorage.lastIndex() + 1, -1)) {
            it.forEachRemaining(e -> counter.incrementAndGet());
        }
        assertEquals(counter.get(), 10);
        counter.set(0);
        try (ILogEntryIterator it = stateStorage.entries(stateStorage.firstIndex(), stateStorage.lastIndex() + 1, 2)) {
            it.forEachRemaining(e -> counter.incrementAndGet());
        }
        assertEquals(counter.get(), 1);

        counter.set(0);
        try (ILogEntryIterator it = stateStorage.entries(stateStorage.firstIndex(), stateStorage.lastIndex() + 1, 10)) {
            it.forEachRemaining(e -> counter.incrementAndGet());
        }
        assertEquals(counter.get(), 2);
    }

    @Test
    public void testAppendClusterConfigEntry() {
        IRaftStateStore stateStorage = setupStateStorage();
        int count = 10;
        while (count-- > 0) {
            LogEntry.Builder entryBuilder = LogEntry.newBuilder();
            if (count == 5) {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setConfig(ClusterConfig.newBuilder()
                        .addVoters("V1")
                        .addVoters("V2")
                        .addVoters("V3")
                        .addVoters("V4")
                        .addVoters("V5")
                        .build());
            } else {
                entryBuilder
                    .setTerm(1)
                    .setIndex(stateStorage.lastIndex() + 1)
                    .setData(ByteString.copyFromUtf8("Data:" + (stateStorage.lastIndex() + 1)));
            }
            stateStorage.append(singletonList(entryBuilder.build()), true);
        }
        assertEquals(stateStorage.latestClusterConfig(), ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .addVoters("V4")
            .addVoters("V5")
            .build());

        stateStorage.append(singletonList(LogEntry.newBuilder()
            .setTerm(1)
            .setIndex(4)
            .setConfig(ClusterConfig.newBuilder()
                .addVoters("S1")
                .addVoters("S2")
                .addVoters("S3")
                .build())
            .build()), true);

        assertEquals(stateStorage.latestClusterConfig(), ClusterConfig.newBuilder()
            .addVoters("S1")
            .addVoters("S2")
            .addVoters("S3")
            .build());
    }

    private IRaftStateStore setupStateStorage() {
        ClusterConfig initialClusterConfig = ClusterConfig.newBuilder()
            .addVoters(localId())
            .addVoters(localId() + "_2")
            .addVoters(localId() + "_3")
            .build();

        Snapshot initSnapshot = Snapshot.newBuilder()
            .setClusterConfig(initialClusterConfig)
            .setIndex(0)
            .setTerm(0)
            .build();
        return createStorage(localId(), initSnapshot);
    }
}
