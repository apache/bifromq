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

package org.apache.bifromq.basekv.store.wal;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.raft.IRaftNode;
import org.apache.bifromq.basekv.raft.event.CommitEvent;
import org.apache.bifromq.basekv.raft.proto.LogEntry;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeWALSubscriptionTest extends MockableTest {
    private final long maxSize = 1024;
    @Mock
    private IKVRangeWAL wal;
    @Mock
    private IKVRangeWALSubscriber subscriber;
    @Mock
    private PublishSubject<IKVRangeWAL.RestoreSnapshotTask> snapshotSource;
    private BehaviorSubject<CommitEvent> commitIndexSource;
    private IRaftNode.IAfterInstalledCallback afterInstalled;
    private ExecutorService executor;

    protected void doSetup(Method method) {
        executor = Executors.newSingleThreadScheduledExecutor();
        commitIndexSource = BehaviorSubject.create();
        snapshotSource = PublishSubject.create();
        when(wal.snapshotRestoreTask()).thenReturn(snapshotSource);
        when(wal.rangeId()).thenReturn(KVRangeIdUtil.generate());
        // provide a non-null raft after-installed callback to avoid NPE when tests trigger it
        afterInstalled = (snapshot, ex) -> CompletableFuture.completedFuture(null);
    }

    protected void doTearDown(Method method) {
        MoreExecutors.shutdownAndAwaitTermination(executor, Duration.ofSeconds(5));
    }

    @SneakyThrows
    @Test
    public void retrieveFailAndRetry() {
        when(wal.retrieveCommitted(0, maxSize)).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("For Testing")),
            CompletableFuture.completedFuture(Iterators.forArray(LogEntry.newBuilder()
                .setTerm(0)
                .setIndex(0)
                .build())));
        CountDownLatch latch = new CountDownLatch(1);
        when(subscriber.apply(any(LogEntry.class), anyBoolean())).thenAnswer(
            (Answer<CompletableFuture<Void>>) invocationOnMock -> {
                latch.countDown();
                return CompletableFuture.completedFuture(null);
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(toCommitEvent(0L, false));
        latch.await();
        verify(wal, times(2)).retrieveCommitted(0, maxSize);
    }

    @SneakyThrows
    @Test
    public void NoRetryWhenIndexOutOfBound() {
        when(wal.retrieveCommitted(0, maxSize)).thenReturn(
            CompletableFuture.failedFuture(new IndexOutOfBoundsException("For Testing")),
            CompletableFuture.completedFuture(Iterators.forArray(LogEntry.newBuilder()
                .setTerm(0)
                .setIndex(0)
                .build())));
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(toCommitEvent(0L, false));
        verify(wal, timeout(1000).times(1)).retrieveCommitted(eq(0L), eq(maxSize));
    }


    @SneakyThrows
    @Test
    public void stopRetryWhenStop() {
        CountDownLatch latch = new CountDownLatch(2);
        when(wal.retrieveCommitted(0, maxSize))
            .thenAnswer((Answer<CompletableFuture<Iterator<LogEntry>>>) invocationOnMock -> {
                latch.countDown();
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException());
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(toCommitEvent(0L, false));
        latch.await();
        walSub.stop();
        verify(wal, atLeast(2)).retrieveCommitted(0, maxSize);
    }

    @SneakyThrows
    @Test
    public void reapplyLog() {
        when(wal.retrieveCommitted(0, maxSize)).thenReturn(CompletableFuture.completedFuture(
            Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(1).build()))
        );
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger applyCount = new AtomicInteger();
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenAnswer((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                log.info("invoke");
                if (applyCount.getAndIncrement() == 0) {
                    return CompletableFuture.failedFuture(new KVRangeException.TryLater("try again"));
                }
                latch.countDown();
                return CompletableFuture.completedFuture(null);
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(toCommitEvent(0L, true));
        commitIndexSource.onNext(toCommitEvent(1L, true));
        latch.await();
        assertTrue(1 < applyCount.get());
        ArgumentCaptor<LogEntry> logEntryCap = ArgumentCaptor.forClass(LogEntry.class);
        await().until(() -> {
            verify(subscriber, atLeast(2)).apply(logEntryCap.capture(), eq(true));
            return logEntryCap.getAllValues().get(0).getIndex() == 0
                && logEntryCap.getAllValues().get(logEntryCap.getAllValues().size() - 1).getIndex() == 1;
        });
    }

    @SneakyThrows
    @Test
    public void cancelApplyLogWhenSnapshot() {
        when(wal.retrieveCommitted(0, maxSize)).thenReturn(CompletableFuture.completedFuture(
            Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(1).build()))
        );
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Void> applyLogFuture = new CompletableFuture<>();
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenAnswer((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                latch.countDown();
                return applyLogFuture;
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(toCommitEvent(0L, false));
        latch.await();
        snapshotSource.onNext(
            new IKVRangeWAL.RestoreSnapshotTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader",
                afterInstalled));
        await().until(applyLogFuture::isCancelled);
    }

    @SneakyThrows
    @Test
    public void cancelReapplyWhenSnapshot() {
        when(wal.retrieveCommitted(0, maxSize)).thenReturn(CompletableFuture.completedFuture(
            Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(1).build()))
        );
        KVRangeSnapshot snapshot = KVRangeSnapshot.getDefaultInstance();
        AtomicInteger retryCount = new AtomicInteger();
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenAnswer((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                retryCount.incrementAndGet();
                return CompletableFuture.failedFuture(new KVRangeException.TryLater("Try again"));
            });
        CountDownLatch latch = new CountDownLatch(1);
        when(subscriber.restore(any(KVRangeSnapshot.class), anyString(), any()))
            .thenAnswer((Answer<CompletableFuture<KVRangeSnapshot>>) invocationOnMock -> {
                latch.countDown();
                return CompletableFuture.completedFuture(snapshot);
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(toCommitEvent(0L, false));
        await().until(() -> retryCount.get() > 2);
        snapshotSource.onNext(
            new IKVRangeWAL.RestoreSnapshotTask(snapshot.toByteString(), "leader", afterInstalled));
        latch.await();
        int c = retryCount.get();
        Thread.sleep(100);
        assertEquals(retryCount.get(), c);
    }

    @SneakyThrows
    @Test
    public void cancelApplySnapshot() {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<KVRangeSnapshot> applySnapshotFuture = new CompletableFuture<>();
        when(subscriber.restore(any(KVRangeSnapshot.class), eq("leader"), any()))
            .thenAnswer((Answer<CompletableFuture<KVRangeSnapshot>>) invocationOnMock -> {
                latch.countDown();
                return applySnapshotFuture;
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, 0, subscriber, executor);
        snapshotSource.onNext(
            new IKVRangeWAL.RestoreSnapshotTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader",
                afterInstalled));
        latch.await();
        snapshotSource.onNext(
            new IKVRangeWAL.RestoreSnapshotTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader",
                afterInstalled));
        await().until(applySnapshotFuture::isCancelled);
    }

    @SneakyThrows
    @Test
    public void cancelApplySnapshotWhenStop() {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<KVRangeSnapshot> applySnapshotFuture = new CompletableFuture<>();
        when(subscriber.restore(any(KVRangeSnapshot.class), anyString(), any()))
            .thenAnswer((Answer<CompletableFuture<KVRangeSnapshot>>) invocationOnMock -> {
                latch.countDown();
                return applySnapshotFuture;
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, 0, subscriber, executor);
        snapshotSource.onNext(
            new IKVRangeWAL.RestoreSnapshotTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader",
                afterInstalled));
        latch.await();
        walSub.stop();
        await().until(applySnapshotFuture::isCancelled);
    }

    @SneakyThrows
    @Test
    public void applyLogsAndSnapshot() {
        LogEntry entry1 = LogEntry.newBuilder().setTerm(0).setIndex(0).build();
        LogEntry entry2 = LogEntry.newBuilder().setTerm(0).setIndex(1).build();
        when(wal.retrieveCommitted(0, maxSize))
            .thenReturn(CompletableFuture.completedFuture(Iterators.forArray(entry1, entry2)));
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Void> applyLogFuture1 = new CompletableFuture<>();
        when(subscriber.apply(eq(entry1), anyBoolean()))
            .thenAnswer((Answer<CompletableFuture<Void>>) invocationOnMock -> {
                latch.countDown();
                return applyLogFuture1;
            });
        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(toCommitEvent(0L, false));
        latch.await();
        snapshotSource.onNext(
            new IKVRangeWAL.RestoreSnapshotTask(KVRangeSnapshot.getDefaultInstance().toByteString(), "leader",
                afterInstalled));
        await().until(applyLogFuture1::isCancelled);
        verify(subscriber, times(1)).apply(any(LogEntry.class), eq(false));
        verify(subscriber, times(1)).restore(eq(KVRangeSnapshot.getDefaultInstance()), eq("leader"), any());
    }

    @SneakyThrows
    @Test
    public void leaderIdentitySegmentation() {
        // Logs: 0,1,2,3; Commits: (1,true) then (3,false)
        when(wal.retrieveCommitted(eq(0L), eq(maxSize))).thenReturn(
            CompletableFuture.completedFuture(Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(1).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(2).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(3).build()
            ))
        );

        List<Long> indices = Collections.synchronizedList(new ArrayList<>());
        List<Boolean> leaders = Collections.synchronizedList(new ArrayList<>());
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenAnswer((Answer<CompletableFuture<Void>>) inv -> {
                LogEntry le = inv.getArgument(0);
                Boolean isLeader = inv.getArgument(1);
                indices.add(le.getIndex());
                leaders.add(isLeader);
                return CompletableFuture.completedFuture(null);
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);

        commitIndexSource.onNext(toCommitEvent(1L, true));
        commitIndexSource.onNext(toCommitEvent(3L, false));

        await().until(() -> indices.size() >= 4);
        assertEquals(indices.size(), 4);
        assertEquals(indices.get(0).longValue(), 0L);
        assertEquals(indices.get(1).longValue(), 1L);
        assertEquals(indices.get(2).longValue(), 2L);
        assertEquals(indices.get(3).longValue(), 3L);

        assertEquals(leaders.get(0), Boolean.TRUE);
        assertEquals(leaders.get(1), Boolean.TRUE);
        assertEquals(leaders.get(2), Boolean.FALSE);
        assertEquals(leaders.get(3), Boolean.FALSE);
    }

    @SneakyThrows
    @Test
    public void notFetchBeyondObservedCommit() {
        // Logs: 0,1,2; only commit up to 1, then later commit 2
        when(wal.retrieveCommitted(eq(0L), eq(maxSize))).thenReturn(
            CompletableFuture.completedFuture(Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(1).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(2).build()
            ))
        );
        when(wal.retrieveCommitted(eq(2L), eq(maxSize))).thenReturn(
            CompletableFuture.completedFuture(Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(2).build()
            ))
        );

        List<Long> indices = Collections.synchronizedList(new ArrayList<>());
        List<Boolean> leaders = Collections.synchronizedList(new ArrayList<>());
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenAnswer((Answer<CompletableFuture<Void>>) inv -> {
                LogEntry le = inv.getArgument(0);
                Boolean isLeader = inv.getArgument(1);
                indices.add(le.getIndex());
                leaders.add(isLeader);
                return CompletableFuture.completedFuture(null);
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);

        // First commit only to 1
        commitIndexSource.onNext(toCommitEvent(1L, false));
        await().until(() -> indices.size() >= 2);
        assertEquals(indices.size(), 2);
        assertEquals(indices.get(0).longValue(), 0L);
        assertEquals(indices.get(1).longValue(), 1L);
        assertEquals(leaders.size(), 2);
        assertEquals(leaders.get(0), Boolean.FALSE);
        assertEquals(leaders.get(1), Boolean.FALSE);

        // Now commit to 2 with isLeader=true
        commitIndexSource.onNext(toCommitEvent(2L, true));
        await().until(() -> indices.size() >= 3);
        assertEquals(indices.get(2).longValue(), 2L);
        assertEquals(leaders.get(2), Boolean.TRUE);
    }

    @SneakyThrows
    @Test
    public void ignoreOldCommitDoesNotFetch() {
        // Apply 0..2 first, then send an older commit(1), no extra fetch should happen
        AtomicInteger retrieveCount = new AtomicInteger();
        when(wal.retrieveCommitted(org.mockito.ArgumentMatchers.anyLong(), eq(maxSize))).thenAnswer(
            (Answer<CompletableFuture<Iterator<LogEntry>>>) inv -> {
                long from = inv.getArgument(0);
                retrieveCount.incrementAndGet();
                if (from == 0L) {
                    return CompletableFuture.completedFuture(Iterators.forArray(
                        LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                        LogEntry.newBuilder().setTerm(0).setIndex(1).build(),
                        LogEntry.newBuilder().setTerm(0).setIndex(2).build()
                    ));
                }
                return CompletableFuture.completedFuture(Iterators.forArray());
            });
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenReturn(CompletableFuture.completedFuture(null));

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);

        commitIndexSource.onNext(toCommitEvent(2L, true));
        await().until(() -> retrieveCount.get() >= 1);

        int before = retrieveCount.get();
        commitIndexSource.onNext(toCommitEvent(1L, false)); // old commit
        // give some time to process, but should not trigger retrieveCommitted again
        Thread.sleep(200);
        assertEquals(retrieveCount.get(), before);
    }

    @SneakyThrows
    @Test
    public void commitArrivesDuringFetchAndContinuesNextRound() {
        // First round: commit(1,true) => apply 0,1 only; Second round: commit(3,false) => apply 2,3
        Map<Long, AtomicInteger> calls = new HashMap<>();
        when(wal.retrieveCommitted(eq(0L), eq(maxSize))).thenAnswer(
            (Answer<CompletableFuture<Iterator<LogEntry>>>) inv -> {
                calls.computeIfAbsent(0L, k -> new AtomicInteger()).incrementAndGet();
                return CompletableFuture.completedFuture(Iterators.forArray(
                    LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                    LogEntry.newBuilder().setTerm(0).setIndex(1).build(),
                    LogEntry.newBuilder().setTerm(0).setIndex(2).build(),
                    LogEntry.newBuilder().setTerm(0).setIndex(3).build()
                ));
            });
        when(wal.retrieveCommitted(eq(2L), eq(maxSize))).thenAnswer(
            (Answer<CompletableFuture<Iterator<LogEntry>>>) inv -> {
                calls.computeIfAbsent(2L, k -> new AtomicInteger()).incrementAndGet();
                return CompletableFuture.completedFuture(Iterators.forArray(
                    LogEntry.newBuilder().setTerm(0).setIndex(2).build(),
                    LogEntry.newBuilder().setTerm(0).setIndex(3).build()
                ));
            });

        List<Long> indices = Collections.synchronizedList(new ArrayList<>());
        List<Boolean> leaders = Collections.synchronizedList(new ArrayList<>());
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenAnswer((Answer<CompletableFuture<Void>>) inv -> {
                LogEntry le = inv.getArgument(0);
                Boolean isLeader = inv.getArgument(1);
                indices.add(le.getIndex());
                leaders.add(isLeader);
                return CompletableFuture.completedFuture(null);
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);

        commitIndexSource.onNext(toCommitEvent(1L, true));
        await().until(() -> indices.size() >= 2);
        assertEquals(indices.size(), 2);
        assertEquals(indices.get(0).longValue(), 0L);
        assertEquals(indices.get(1).longValue(), 1L);
        assertEquals(leaders.size(), 2);
        assertEquals(leaders.get(0), Boolean.TRUE);
        assertEquals(leaders.get(1), Boolean.TRUE);

        commitIndexSource.onNext(toCommitEvent(3L, false));
        await().until(() -> indices.size() >= 4);
        assertEquals(indices.size(), 4);
        assertEquals(indices.get(0).longValue(), 0L);
        assertEquals(indices.get(1).longValue(), 1L);
        assertEquals(indices.get(2).longValue(), 2L);
        assertEquals(indices.get(3).longValue(), 3L);
        assertEquals(leaders.size(), 4);
        assertEquals(leaders.get(0), Boolean.TRUE);
        assertEquals(leaders.get(1), Boolean.TRUE);
        assertEquals(leaders.get(2), Boolean.FALSE);
        assertEquals(leaders.get(3), Boolean.FALSE);

        assertEquals(calls.getOrDefault(0L, new AtomicInteger()).get(), 1);
        assertEquals(calls.getOrDefault(2L, new AtomicInteger()).get(), 1);
    }

    @SneakyThrows
    @Test
    public void snapshotResetsIndexAndClearsPending() {
        // Install a snapshot with lastAppliedIndex=5, then commit 6 and ensure fetch starts from 6
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenReturn(CompletableFuture.completedFuture(null));

        AtomicInteger calledFrom6 = new AtomicInteger();
        // Default: for non-6 fromIndex, return empty iterator to avoid NPE/stall before snapshot finishes
        when(wal.retrieveCommitted(org.mockito.ArgumentMatchers.longThat(v -> v != 6L), eq(maxSize)))
            .thenAnswer((Answer<CompletableFuture<Iterator<LogEntry>>>) inv ->
                CompletableFuture.completedFuture(Iterators.forArray())
            );
        when(wal.retrieveCommitted(eq(6L), eq(maxSize))).thenAnswer(
            (Answer<CompletableFuture<Iterator<LogEntry>>>) inv -> {
                calledFrom6.incrementAndGet();
                return CompletableFuture.completedFuture(Iterators.forArray(
                    LogEntry.newBuilder().setTerm(0).setIndex(6).build()
                ));
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);

        // Emit snapshot restore task with installed snapshot lastAppliedIndex=5
        KVRangeSnapshot installed = KVRangeSnapshot.newBuilder().setLastAppliedIndex(5).build();
        CountDownLatch latch = new CountDownLatch(1);
        when(subscriber.restore(any(KVRangeSnapshot.class), anyString(), any()))
            .thenAnswer((Answer<CompletableFuture<Void>>) inv -> {
                IKVRangeWALSubscriber.IAfterRestoredCallback cb = inv.getArgument(2);
                cb.call(installed, null);
                latch.countDown();
                return CompletableFuture.completedFuture(null);
            });

        snapshotSource.onNext(
            new IKVRangeWAL.RestoreSnapshotTask(installed.toByteString(), "leader", afterInstalled));
        latch.await();
        // Now a new commit at 6 should trigger fetch from 6
        await().untilAsserted(() -> {
                commitIndexSource.onNext(toCommitEvent(6L, true));
                await().until(() -> calledFrom6.get() >= 1);
            }
        );
        verify(subscriber, timeout(1000).times(1)).apply(eq(LogEntry.newBuilder().setTerm(0).setIndex(6).build()),
            eq(true));
    }

    @SneakyThrows
    @Test
    public void sameLeaderCommitCompression() {
        // Logs: 0,1,2; Commits: (1,true) then (2,true). All applies with true.
        when(wal.retrieveCommitted(eq(0L), eq(maxSize))).thenReturn(
            CompletableFuture.completedFuture(Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(0).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(1).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(2).build()
            ))
        );
        when(wal.retrieveCommitted(eq(2L), eq(maxSize))).thenReturn(
            CompletableFuture.completedFuture(Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(2).build()
            ))
        );

        List<Boolean> leaders = Collections.synchronizedList(new ArrayList<>());
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenAnswer((Answer<CompletableFuture<Void>>) inv -> {
                leaders.add(inv.getArgument(1));
                return CompletableFuture.completedFuture(null);
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, -1, subscriber, executor);
        commitIndexSource.onNext(toCommitEvent(1L, true));
        commitIndexSource.onNext(toCommitEvent(2L, true));

        await().until(() -> leaders.size() >= 3);
        assertEquals(leaders.size(), 3);
        assertEquals(leaders.get(0), Boolean.TRUE);
        assertEquals(leaders.get(1), Boolean.TRUE);
        assertEquals(leaders.get(2), Boolean.TRUE);
    }

    @SneakyThrows
    @Test
    public void startWithCustomLastFetchedIndex() {
        // Start from lastFetchedIndex=10; after commit(12,false), fetch from 11 and apply 11,12 with false.
        when(wal.retrieveCommitted(eq(11L), eq(maxSize))).thenReturn(
            CompletableFuture.completedFuture(Iterators.forArray(
                LogEntry.newBuilder().setTerm(0).setIndex(11).build(),
                LogEntry.newBuilder().setTerm(0).setIndex(12).build()
            ))
        );
        List<Long> indices = Collections.synchronizedList(new ArrayList<>());
        List<Boolean> leaders = Collections.synchronizedList(new ArrayList<>());
        when(subscriber.apply(any(LogEntry.class), anyBoolean()))
            .thenAnswer((Answer<CompletableFuture<Void>>) inv -> {
                indices.add(((LogEntry) inv.getArgument(0)).getIndex());
                leaders.add(inv.getArgument(1));
                return CompletableFuture.completedFuture(null);
            });

        KVRangeWALSubscription walSub =
            new KVRangeWALSubscription(maxSize, wal, commitIndexSource, 10, subscriber, executor);

        commitIndexSource.onNext(toCommitEvent(12L, false));
        await().until(() -> indices.size() >= 2);

        assertEquals(indices.size(), 2);
        assertEquals(indices.get(0).longValue(), 11L);
        assertEquals(indices.get(1).longValue(), 12L);
        assertEquals(leaders.size(), 2);
        assertEquals(leaders.get(0), Boolean.FALSE);
        assertEquals(leaders.get(1), Boolean.FALSE);
    }

    private CommitEvent toCommitEvent(long idx, boolean isLeader) {
        return new CommitEvent("", idx, isLeader);
    }
}
