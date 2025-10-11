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

package org.apache.bifromq.basekv.store.range;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertSame;

import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataReply;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataRequest;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KVRangeRestorerTest {
    private KVRangeId rangeId;
    private IKVRange range;
    private IKVRangeMessenger messenger;
    private PublishSubject<KVRangeMessage> messageSubject;
    private IKVRangeMetricManager metricManager;
    private Executor executor;
    private KVRangeSnapshot snapshot;

    @BeforeMethod
    public void setUp() {
        rangeId = KVRangeIdUtil.generate();
        messageSubject = PublishSubject.create();
        range = mock(IKVRange.class);
        when(range.id()).thenReturn(rangeId);
        messenger = mock(IKVRangeMessenger.class);
        when(messenger.receive()).thenReturn(messageSubject);
        metricManager = mock(IKVRangeMetricManager.class);
        executor = Executors.newSingleThreadExecutor();
        snapshot = KVRangeSnapshot.newBuilder().setId(rangeId).build();
    }

    @AfterMethod
    public void tearDown() {
        if (executor instanceof ExecutorService) {
            MoreExecutors.shutdownAndAwaitTermination((ExecutorService) executor, 5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void awaitDone() {
        IKVRangeRestoreSession restoreSession = mock(IKVRangeRestoreSession.class);
        when(range.startRestore(eq(snapshot), any())).thenReturn(restoreSession);
        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);

        assertTrue(restorer.awaitDone().isDone());

        // Manually complete the future
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);
        assertFalse(restorer.awaitDone().isDone());
        // Ensure no active subscription remains after test
        restoreFuture.cancel(true);
    }

    @Test
    public void restoreFrom() {
        IKVRangeRestoreSession restoreSession = mock(IKVRangeRestoreSession.class);
        when(range.startRestore(eq(snapshot), any())).thenReturn(restoreSession);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);

        ArgumentCaptor<KVRangeMessage> messageCaptor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCaptor.capture());
        KVRangeMessage message = messageCaptor.getValue();
        assertTrue(message.hasSnapshotSyncRequest());

        // Simulate receiving snapshot data
        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setHostStoreId("leader")
            .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                .setSessionId(message.getSnapshotSyncRequest().getSessionId())
                .setFlag(SaveSnapshotDataRequest.Flag.End)
                .build())
            .build());

        // Wait for the future to complete
        restoreFuture.join();
        // Verify the reseter's put and done methods were called
        verify(restoreSession, times(1)).done();
    }

    @Test
    public void restoreFromWithError() {
        IKVRangeRestoreSession restoreSession = mock(IKVRangeRestoreSession.class);
        when(range.startRestore(eq(snapshot), any())).thenReturn(restoreSession);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);

        ArgumentCaptor<KVRangeMessage> messageCaptor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCaptor.capture());
        KVRangeMessage message = messageCaptor.getValue();
        assertTrue(message.hasSnapshotSyncRequest());
        String sessionId = message.getSnapshotSyncRequest().getSessionId();

        // Simulate receiving snapshot data
        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setHostStoreId("leader")
            .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                .setSessionId(sessionId)
                .setFlag(SaveSnapshotDataRequest.Flag.Error)
                .build())
            .build());

        // Wait for the future to complete
        assertThrows(restoreFuture::join);

        // Verify the reseter's put and done methods were called
        verify(restoreSession, times(1)).abort();
    }

    @Test
    public void restoreFromTimeout() {
        IKVRangeRestoreSession restoreSession = mock(IKVRangeRestoreSession.class);
        when(range.startRestore(eq(snapshot), any())).thenReturn(restoreSession);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 1);
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);

        // Wait for the future to complete
        assertThrows(restoreFuture::join);

        // Verify the reseter's put and done methods were called
        verify(restoreSession, times(1)).abort();
    }

    @Test
    public void cancelPreviousSession() {
        IKVRangeRestoreSession firstRS = mock(IKVRangeRestoreSession.class);
        IKVRangeRestoreSession secondRS = mock(IKVRangeRestoreSession.class);
        when(range.startRestore(eq(snapshot), any())).thenReturn(firstRS);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);

        // Start the first restore session
        CompletableFuture<Void> firstRestore = restorer.restoreFrom("leader", snapshot);

        // Ensure receiver subscribed before starting next session
        assertTrue(messageSubject.hasObservers()); // ensure receiver subscribed

        // Start the second restore session, which should cancel the first
        KVRangeSnapshot newSnapshot = KVRangeSnapshot.newBuilder().setId(snapshot.getId()).setVer(1).build();
        when(range.startRestore(eq(newSnapshot), any())).thenReturn(secondRS);
        CompletableFuture<Void> secondRestore = restorer.restoreFrom("leader", newSnapshot);

        verify(firstRS, atLeast(1)).abort();
        verify(secondRS, times(0)).abort();
        // Allow async cancellation hook to send NoSessionFound
        verify(messenger, timeout(500)).send(argThat(m ->
            m.hasSaveSnapshotDataReply() &&
                m.getSaveSnapshotDataReply().getResult() == SaveSnapshotDataReply.Result.NoSessionFound));
        assertTrue(firstRestore.isCancelled());
        assertFalse(secondRestore.isDone());

        secondRestore.cancel(true);
    }

    @Test
    public void reuseRestoreSessionForSameSnapshot() {
        IKVRangeRestoreSession restoreSession = mock(IKVRangeRestoreSession.class);
        when(range.startRestore(eq(snapshot), any())).thenReturn(restoreSession);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);

        CompletableFuture<Void> firstRestore = restorer.restoreFrom("leader", snapshot);
        CompletableFuture<Void> secondRestore = restorer.restoreFrom("leader", snapshot);

        assertSame(firstRestore, secondRestore);
        verify(range, times(1)).startRestore(eq(snapshot), any());
        verify(messenger, times(1)).send(argThat(KVRangeMessage::hasSnapshotSyncRequest));

        ArgumentCaptor<KVRangeMessage> messageCaptor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCaptor.capture());
        KVRangeMessage message = messageCaptor.getValue();

        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setHostStoreId("leader")
            .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                .setSessionId(message.getSnapshotSyncRequest().getSessionId())
                .setFlag(SaveSnapshotDataRequest.Flag.End)
                .build())
            .build());

        firstRestore.join();
        assertTrue(secondRestore.isDone());
        verify(restoreSession, times(1)).done();
    }

    @Test
    public void startNewSessionWhenLeaderChanges() {
        IKVRangeRestoreSession firstRS = mock(IKVRangeRestoreSession.class);
        IKVRangeRestoreSession secondRS = mock(IKVRangeRestoreSession.class);
        when(range.startRestore(eq(snapshot), any())).thenReturn(firstRS, secondRS);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);

        CompletableFuture<Void> firstRestore = restorer.restoreFrom("leader1", snapshot);
        CompletableFuture<Void> secondRestore = restorer.restoreFrom("leader2", snapshot);

        assertTrue(firstRestore.isCancelled());
        assertFalse(secondRestore.isDone());
        verify(range, times(2)).startRestore(eq(snapshot), any());
        verify(messenger, times(2)).send(argThat(KVRangeMessage::hasSnapshotSyncRequest));
        verify(firstRS, atLeast(1)).abort();
        // Cleanup: cancel the second pending restore to avoid executor use after shutdown
        secondRestore.cancel(true); // ensure no lingering scheduling
    }

    @Test
    public void reuseAfterDoneStartsNew() {
        IKVRangeRestoreSession firstRS = mock(IKVRangeRestoreSession.class);
        IKVRangeRestoreSession secondRS = mock(IKVRangeRestoreSession.class);
        when(range.startRestore(eq(snapshot), any())).thenReturn(firstRS, secondRS);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);

        // Start first restore
        CompletableFuture<Void> firstRestore = restorer.restoreFrom("leader", snapshot);
        ArgumentCaptor<KVRangeMessage> captor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(captor.capture());
        String sessionId1 = captor.getValue().getSnapshotSyncRequest().getSessionId();

        // Complete first restore
        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setHostStoreId("leader")
            .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                .setSessionId(sessionId1)
                .setFlag(SaveSnapshotDataRequest.Flag.End)
                .build())
            .build());
        firstRestore.join();

        // Start second restore with same leader and snapshot; should start a new session
        CompletableFuture<Void> secondRestore = restorer.restoreFrom("leader", snapshot);

        // Only count SnapshotSyncRequest sends; receiver also sends replies
        verify(messenger, times(2)).send(argThat(KVRangeMessage::hasSnapshotSyncRequest));
        // Capture all sends, then filter out SnapshotSyncRequest to compare session ids
        verify(messenger, atLeast(2)).send(captor.capture());
        String sessionId2 = captor.getAllValues().stream()
            .filter(KVRangeMessage::hasSnapshotSyncRequest)
            .map(m -> m.getSnapshotSyncRequest().getSessionId())
            .reduce((first, second) -> second)
            .orElse("");

        // Verify new startRestore invoked and session id differs
        verify(range, times(2)).startRestore(eq(snapshot), any());
        assertFalse(sessionId1.equals(sessionId2));

        secondRestore.cancel(true);
    }
}
