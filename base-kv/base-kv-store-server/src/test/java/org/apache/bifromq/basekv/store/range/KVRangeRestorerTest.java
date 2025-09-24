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

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertSame;

import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.bifromq.basekv.proto.KVPair;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.KVRangeSnapshot;
import org.apache.bifromq.basekv.proto.SaveSnapshotDataRequest;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KVRangeRestorerTest {
    private KVRangeId rangeId;
    private IKVRange range;
    private IKVReseter reseter;
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

    @Test
    public void awaitDone() {
        IKVReseter reseter = mock(IKVReseter.class);
        when(range.toReseter(eq(snapshot))).thenReturn(reseter);
        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);

        assertTrue(restorer.awaitDone().isDone());

        // Manually complete the future
        restorer.restoreFrom("leader", snapshot);
        assertFalse(restorer.awaitDone().isDone());
    }

    @Test
    public void restoreFrom() {
        IKVReseter reseter = mock(IKVReseter.class);
        when(range.toReseter(snapshot)).thenReturn(reseter);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);

        ArgumentCaptor<KVRangeMessage> messageCaptor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCaptor.capture());
        KVRangeMessage message = messageCaptor.getValue();
        assertTrue(message.hasSnapshotSyncRequest());

        // Simulate receiving snapshot data
        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                .setSessionId(message.getSnapshotSyncRequest().getSessionId())
                .setFlag(SaveSnapshotDataRequest.Flag.End)
                .build())
            .build());

        // Wait for the future to complete
        restoreFuture.join();
        // Verify the reseter's put and done methods were called
        verify(reseter, times(1)).done();
    }

    @Test
    public void restoreFromWithError() {
        IKVReseter reseter = mock(IKVReseter.class);
        when(range.toReseter(snapshot)).thenReturn(reseter);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);

        ArgumentCaptor<KVRangeMessage> messageCaptor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCaptor.capture());
        KVRangeMessage message = messageCaptor.getValue();
        assertTrue(message.hasSnapshotSyncRequest());
        String sessionId = message.getSnapshotSyncRequest().getSessionId();

        // Simulate receiving snapshot data
        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                .setSessionId(sessionId)
                .setFlag(SaveSnapshotDataRequest.Flag.Error)
                .build())
            .build());

        // Wait for the future to complete
        assertThrows(restoreFuture::join);

        // Verify the reseter's put and done methods were called
        verify(reseter, times(1)).abort();
    }

    @Test
    public void restoreFromTimeout() {
        IKVReseter reseter = mock(IKVReseter.class);
        when(range.toReseter(eq(snapshot))).thenReturn(reseter);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 1);
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);

        // Wait for the future to complete
        assertThrows(restoreFuture::join);

        // Verify the reseter's put and done methods were called
        verify(reseter, times(1)).abort();
    }

    @Test
    public void cancelPreviousSession() {
        IKVReseter reseter = mock(IKVReseter.class);
        when(range.toReseter(eq(snapshot))).thenReturn(reseter);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);

        // Start the first restore session
        CompletableFuture<Void> firstRestore = restorer.restoreFrom("leader", snapshot);

        // Start the second restore session, which should cancel the first
        KVRangeSnapshot newSnapshot = KVRangeSnapshot.newBuilder().setId(snapshot.getId()).setVer(1).build();
        when(range.toReseter(eq(newSnapshot))).thenReturn(reseter);
        CompletableFuture<Void> secondRestore = restorer.restoreFrom("leader", newSnapshot);

        verify(reseter, times(1)).abort();
        assertTrue(firstRestore.isCancelled());
        assertFalse(secondRestore.isDone());
    }

    @Test
    public void reuseRestoreSessionForSameSnapshot() {
        IKVReseter reseter = mock(IKVReseter.class);
        when(range.toReseter(snapshot)).thenReturn(reseter);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);

        CompletableFuture<Void> firstRestore = restorer.restoreFrom("leader", snapshot);
        CompletableFuture<Void> secondRestore = restorer.restoreFrom("leader", snapshot);

        assertSame(firstRestore, secondRestore);
        verify(range, times(1)).toReseter(snapshot);
        verify(messenger, times(1)).send(argThat(KVRangeMessage::hasSnapshotSyncRequest));

        ArgumentCaptor<KVRangeMessage> messageCaptor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCaptor.capture());
        KVRangeMessage message = messageCaptor.getValue();

        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                .setSessionId(message.getSnapshotSyncRequest().getSessionId())
                .setFlag(SaveSnapshotDataRequest.Flag.End)
                .build())
            .build());

        firstRestore.join();
        assertTrue(secondRestore.isDone());
        verify(reseter, times(1)).done();
    }

    @Test
    public void startNewSessionWhenLeaderChanges() {
        IKVReseter firstReseter = mock(IKVReseter.class);
        IKVReseter secondReseter = mock(IKVReseter.class);
        when(range.toReseter(snapshot)).thenReturn(firstReseter, secondReseter);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);

        CompletableFuture<Void> firstRestore = restorer.restoreFrom("leader1", snapshot);
        CompletableFuture<Void> secondRestore = restorer.restoreFrom("leader2", snapshot);

        assertTrue(firstRestore.isCancelled());
        assertFalse(secondRestore.isDone());
        verify(range, times(2)).toReseter(snapshot);
        verify(messenger, times(2)).send(argThat(KVRangeMessage::hasSnapshotSyncRequest));
        verify(firstReseter, times(1)).abort();
    }

    @Test
    public void autoFlushWhenEntryLimitReached() {
        IKVReseter reseter = mock(IKVReseter.class);
        when(range.toReseter(eq(snapshot))).thenReturn(reseter);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);

        ArgumentCaptor<KVRangeMessage> messageCaptor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCaptor.capture());
        String sessionId = messageCaptor.getValue().getSnapshotSyncRequest().getSessionId();

        SaveSnapshotDataRequest.Builder builder = SaveSnapshotDataRequest.newBuilder()
            .setSessionId(sessionId)
            .setFlag(SaveSnapshotDataRequest.Flag.End);
        for (int i = 0; i < 1_025; i++) {
            builder.addKv(KVPair.newBuilder()
                .setKey(ByteString.copyFromUtf8("k" + i))
                .setValue(ByteString.copyFromUtf8("v" + i))
                .build());
        }

        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setSaveSnapshotDataRequest(builder.build())
            .build());

        restoreFuture.join();

        verify(reseter, atLeast(2)).flush();
        verify(reseter, times(1)).done();
    }

    @Test
    public void flushRemainingDataOnEnd() {
        IKVReseter reseter = mock(IKVReseter.class);
        when(range.toReseter(eq(snapshot))).thenReturn(reseter);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);

        ArgumentCaptor<KVRangeMessage> messageCaptor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCaptor.capture());
        String sessionId = messageCaptor.getValue().getSnapshotSyncRequest().getSessionId();

        KVPair kv1 = KVPair.newBuilder()
            .setKey(ByteString.copyFromUtf8("k1"))
            .setValue(ByteString.copyFromUtf8("v1"))
            .build();

        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setSaveSnapshotDataRequest(SaveSnapshotDataRequest.newBuilder()
                .setSessionId(sessionId)
                .setFlag(SaveSnapshotDataRequest.Flag.End)
                .addKv(kv1)
                .build())
            .build());

        restoreFuture.join();

        verify(reseter, times(1)).flush();
        verify(reseter, times(1)).done();
    }

    @Test
    public void adaptiveFlushWithUnlimitedBudget() {
        IKVReseter reseter = mock(IKVReseter.class);
        when(range.toReseter(eq(snapshot))).thenReturn(reseter);

        KVRangeRestorer restorer = new KVRangeRestorer(snapshot, range, messenger, metricManager, executor, 10);
        CompletableFuture<Void> restoreFuture = restorer.restoreFrom("leader", snapshot);

        ArgumentCaptor<KVRangeMessage> messageCaptor = ArgumentCaptor.forClass(KVRangeMessage.class);
        verify(messenger).send(messageCaptor.capture());
        String sessionId = messageCaptor.getValue().getSnapshotSyncRequest().getSessionId();

        SaveSnapshotDataRequest.Builder builder = SaveSnapshotDataRequest.newBuilder()
            .setSessionId(sessionId)
            .setFlag(SaveSnapshotDataRequest.Flag.End);
        for (int i = 0; i < 2048; i++) {
            builder.addKv(KVPair.newBuilder()
                .setKey(ByteString.copyFromUtf8("k" + i))
                .setValue(ByteString.copyFromUtf8("v" + i))
                .build());
        }

        messageSubject.onNext(KVRangeMessage.newBuilder()
            .setSaveSnapshotDataRequest(builder.build())
            .build());

        restoreFuture.join();

        verify(reseter, atLeast(2)).flush();
        verify(reseter, times(1)).done();
    }
}