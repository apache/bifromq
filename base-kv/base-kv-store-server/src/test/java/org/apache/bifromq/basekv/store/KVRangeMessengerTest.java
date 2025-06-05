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

package org.apache.bifromq.basekv.store;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.basekv.MockableTest;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.StoreMessage;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.testng.annotations.Test;

@Slf4j
public class KVRangeMessengerTest extends MockableTest {
    @Mock
    private IStoreMessenger storeMessenger;
    private PublishSubject<StoreMessage> incomingStoreMsg;

    protected void doSetup(Method method) {
        incomingStoreMsg = PublishSubject.create();
    }

    protected void doTearDown(Method method) {
    }

    @Test
    public void send() {
        String srcStoreId = "srcStoreId";
        KVRangeId srcRangeId = KVRangeIdUtil.generate();
        KVRangeMessenger messenger = new KVRangeMessenger(srcStoreId, srcRangeId, storeMessenger);

        messenger.send(KVRangeMessage.getDefaultInstance());
        ArgumentCaptor<StoreMessage> messageCap = ArgumentCaptor.forClass(StoreMessage.class);
        verify(storeMessenger).send(messageCap.capture());
        assertEquals(messageCap.getValue().getFrom(), srcStoreId);
        assertEquals(messageCap.getValue().getSrcRange(), srcRangeId);
    }

    @Test
    public void receiveSend() {
        String srcStoreId = "srcStoreId";
        KVRangeId srcRangeId = KVRangeIdUtil.generate();
        String targetStoreId = "targetStoreId";
        KVRangeId targetRangeId = KVRangeIdUtil.generate();
        when(storeMessenger.receive()).thenReturn(incomingStoreMsg);
        KVRangeMessenger messenger = new KVRangeMessenger(targetStoreId, targetRangeId, storeMessenger);
        TestObserver<KVRangeMessage> rangeMsgObserver = TestObserver.create();
        messenger.receive().subscribe(rangeMsgObserver);

        KVRangeMessage rangeMessage = KVRangeMessage.newBuilder()
            .setHostStoreId(targetStoreId)
            .setRangeId(targetRangeId)
            .build();
        StoreMessage storeMessage = StoreMessage.newBuilder()
            .setFrom(srcStoreId)
            .setSrcRange(srcRangeId)
            .setPayload(rangeMessage)
            .build();
        incomingStoreMsg.onNext(storeMessage);
        rangeMsgObserver.awaitCount(1);

        KVRangeMessage receivedMsg = rangeMsgObserver.values().get(0);
        assertEquals(receivedMsg.getHostStoreId(), srcStoreId);
        assertEquals(receivedMsg.getRangeId(), srcRangeId);
    }

    @SneakyThrows
    @Test
    public void ignoreWrongTarget() {
        String srcStoreId = "srcStoreId";
        KVRangeId srcRangeId = KVRangeIdUtil.generate();
        String targetStoreId = "targetStoreId";
        String targetStoreId1 = "targetStoreId1";
        KVRangeId targetRangeId = KVRangeIdUtil.generate();
        KVRangeId targetRangeId1 = KVRangeIdUtil.generate();
        when(storeMessenger.receive()).thenReturn(incomingStoreMsg);
        KVRangeMessenger messenger = new KVRangeMessenger(targetStoreId, targetRangeId, storeMessenger);
        TestObserver<KVRangeMessage> rangeMsgObserver = TestObserver.create();
        messenger.receive().subscribe(rangeMsgObserver);

        KVRangeMessage rangeMessage = KVRangeMessage.newBuilder()
            .setHostStoreId(targetStoreId)
            .setRangeId(targetRangeId1)
            .build();
        StoreMessage storeMessage = StoreMessage.newBuilder()
            .setFrom(srcStoreId)
            .setSrcRange(srcRangeId)
            .setPayload(rangeMessage)
            .build();
        incomingStoreMsg.onNext(storeMessage);
        rangeMsgObserver.await(100, TimeUnit.MILLISECONDS);
        assertEquals(rangeMsgObserver.values().size(), 0);

        rangeMessage = KVRangeMessage.newBuilder()
            .setHostStoreId(targetStoreId1)
            .setRangeId(targetRangeId)
            .build();
        storeMessage = StoreMessage.newBuilder()
            .setFrom(srcStoreId)
            .setSrcRange(srcRangeId)
            .setPayload(rangeMessage)
            .build();
        incomingStoreMsg.onNext(storeMessage);
        rangeMsgObserver.await(100, TimeUnit.MILLISECONDS);
        assertEquals(rangeMsgObserver.values().size(), 0);
    }

    @Test
    public void once() {
        String srcStoreId = "srcStoreId";
        KVRangeId srcRangeId = KVRangeIdUtil.generate();
        String targetStoreId = "targetStoreId";
        KVRangeId targetRangeId = KVRangeIdUtil.generate();
        when(storeMessenger.receive()).thenReturn(incomingStoreMsg);
        KVRangeMessenger messenger = new KVRangeMessenger(targetStoreId, targetRangeId, storeMessenger);
        TestObserver<KVRangeMessage> rangeMsgObserver = TestObserver.create();
        messenger.receive().subscribe(rangeMsgObserver);

        KVRangeMessage rangeMessage = KVRangeMessage.newBuilder()
            .setHostStoreId(targetStoreId)
            .setRangeId(targetRangeId)
            .build();
        StoreMessage storeMessage = StoreMessage.newBuilder()
            .setFrom(srcStoreId)
            .setSrcRange(srcRangeId)
            .setPayload(rangeMessage)
            .build();
        CompletableFuture<KVRangeMessage> onceFuture = messenger.once(msg -> true);
        incomingStoreMsg.onNext(storeMessage);
        await().until(() -> onceFuture.isDone() && !onceFuture.isCompletedExceptionally() &&
            onceFuture.join().equals(KVRangeMessage.newBuilder()
                .setRangeId(srcRangeId)
                .setHostStoreId(srcStoreId)
                .build()));

        CompletableFuture<KVRangeMessage> onceFuture1 = messenger.once(msg -> true);
        incomingStoreMsg.onComplete();
        await().until(() -> onceFuture1.isCompletedExceptionally());
    }
}
