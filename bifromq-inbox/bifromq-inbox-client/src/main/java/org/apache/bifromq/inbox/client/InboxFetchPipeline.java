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

package org.apache.bifromq.inbox.client;

import static org.apache.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_DELIVERERKEY;
import static org.apache.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_ID;

import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.baserpc.client.IRPCClient.IMessageStream;
import org.apache.bifromq.inbox.rpc.proto.InboxFetchHint;
import org.apache.bifromq.inbox.rpc.proto.InboxFetched;
import org.apache.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxFetchPipeline {
    private static final Cleaner CLEANER = Cleaner.create();

    private final IMessageStream<InboxFetched, InboxFetchHint> messageStream;
    private final String tenantId;
    private final Map<InboxFetchSessionId, Consumer<Fetched>> fetcherMap = new ConcurrentHashMap<>();
    private final Cleanable cleanable;

    InboxFetchPipeline(String tenantId, String delivererKey, IRPCClient rpcClient) {
        this.tenantId = tenantId;
        this.messageStream = rpcClient.createMessageStream(tenantId, null, delivererKey,
            Map.of(PIPELINE_ATTR_KEY_ID, UUID.randomUUID().toString(), PIPELINE_ATTR_KEY_DELIVERERKEY, delivererKey),
            InboxServiceGrpc.getFetchMethod());
        this.cleanable = CLEANER.register(this, new PipelineCloseAction(messageStream));
        this.messageStream.onMessage(new MessageListener(fetcherMap));
        this.messageStream.onRetarget(new RetargetListener(fetcherMap));
    }

    public void fetch(long sessionId, String inboxId, long incarnation, Consumer<Fetched> consumer) {
        fetcherMap.put(new InboxFetchSessionId(sessionId, inboxId, incarnation), consumer);
    }

    public void stopFetch(long sessionId, String inboxId, long incarnation) {
        fetcherMap.remove(new InboxFetchSessionId(sessionId, inboxId, incarnation));
    }

    public void hint(long sessionId,
                     String inboxId,
                     long incarnation,
                     int bufferCapacity,
                     long lastFetchQoS0Seq,
                     long lastFetchSendBufferSeq) {
        log.trace(
            "Send hint: inboxId={}, incarnation={}, capacity={}, client={}, lastFetchedQoSeq={}, lastFetchedSendBufferSeq={}",
            inboxId, incarnation, bufferCapacity, tenantId, lastFetchQoS0Seq, lastFetchSendBufferSeq);
        messageStream.ack(InboxFetchHint.newBuilder()
            .setSessionId(sessionId)
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setCapacity(bufferCapacity)
            .setLastFetchQoS0Seq(lastFetchQoS0Seq)
            .setLastFetchSendBufferSeq(lastFetchSendBufferSeq)
            .build());
    }

    public void close() {
        if (cleanable != null) {
            cleanable.clean();
        }
    }

    private record MessageListener(Map<InboxFetchSessionId, Consumer<Fetched>> fetcherMap)
        implements Consumer<InboxFetched> {

        @Override
        public void accept(InboxFetched inboxFetched) {
            Consumer<Fetched> fetcher = fetcherMap.get(
                new InboxFetchSessionId(inboxFetched.getSessionId(),
                    inboxFetched.getInboxId(),
                    inboxFetched.getIncarnation()));
            if (fetcher != null) {
                Fetched fetched = inboxFetched.getFetched();
                fetcher.accept(fetched);
            }
        }
    }

    private record RetargetListener(Map<InboxFetchSessionId, Consumer<Fetched>> fetcherMap) implements Consumer<Long> {

        @Override
        public void accept(Long ts) {
            log.debug("Message stream retargeting, signal transient error to all fetchers");
            fetcherMap.values().forEach(consumer -> consumer.accept(Fetched.newBuilder()
                .setResult(Fetched.Result.TRY_LATER)
                .build()));
        }
    }

    private record PipelineCloseAction(IMessageStream<InboxFetched, InboxFetchHint> messageStream) implements Runnable {
        @Override
        public void run() {
            messageStream.close();
        }
    }

    private record InboxFetchSessionId(long sessionId, String inboxId, long incarnation) {
    }
}
