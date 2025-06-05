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

import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeMessage;
import org.apache.bifromq.basekv.proto.StoreMessage;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.range.IKVRangeMessenger;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class KVRangeMessenger implements IKVRangeMessenger {
    private final String id;
    private final KVRangeId rangeId;
    private final IStoreMessenger messenger;

    public KVRangeMessenger(String id, KVRangeId rangeId, IStoreMessenger messenger) {
        this.id = id;
        this.rangeId = rangeId;
        this.messenger = messenger;
    }

    @Override
    public void send(KVRangeMessage message) {
        messenger.send(StoreMessage.newBuilder()
            .setFrom(id)
            .setSrcRange(rangeId)
            .setPayload(message)
            .build());
    }

    @Override
    public Observable<KVRangeMessage> receive() {
        return messenger.receive().mapOptional(storeMessage -> {
            assert storeMessage.getFrom() != null;
            assert storeMessage.hasSrcRange();
            KVRangeMessage payload = storeMessage.getPayload();
            if (!payload.getHostStoreId().equals(id) || !payload.getRangeId().equals(rangeId)) {
                return Optional.empty();
            }
            // swap the origin
            return Optional.of(payload.toBuilder()
                .setRangeId(storeMessage.getSrcRange())
                .setHostStoreId(storeMessage.getFrom())
                .build());
        });
    }

    @Override
    public CompletableFuture<KVRangeMessage> once(Predicate<KVRangeMessage> condition) {
        CompletableFuture<KVRangeMessage> onDone = new CompletableFuture<>();
        Disposable disposable = receive()
            .mapOptional(msg -> {
                if (condition.test(msg)) {
                    return Optional.of(msg);
                }
                return Optional.empty();
            })
            .firstElement()
            .subscribe(onDone::complete,
                e -> onDone.completeExceptionally(new KVRangeException.TryLater("Once test canceled", e)),
                () -> {
                    if (!onDone.isDone()) {
                        onDone.completeExceptionally(new KVRangeException.TryLater("Try again"));
                    }
                });

        onDone.whenComplete((v, e) -> disposable.dispose());
        return onDone;
    }
}
