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

package org.apache.bifromq.inbox.client;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.inbox.rpc.proto.CommitReply;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.rpc.proto.InboxFetchHint;
import org.apache.bifromq.inbox.rpc.proto.InboxFetched;
import org.apache.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.Fetched.Result;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxReaderTest {
    private final String tenantId = "tenantId";
    private final String delivererKey = "delivererKey";
    private final String inboxId = "inboxId";
    private final long incarnation = 1;
    private AutoCloseable closeable;
    @Mock
    private Consumer<Fetched> onFetched;
    @Mock
    private IRPCClient rpcClient;
    @Mock
    private IRPCClient.IMessageStream<InboxFetched, InboxFetchHint> messageStream;
    private InboxFetchPipeline fetchPipeline;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        when(rpcClient.createMessageStream(eq(tenantId), isNull(), eq(delivererKey), anyMap(),
            eq(InboxServiceGrpc.getFetchMethod()))).thenReturn(messageStream);
        fetchPipeline = new InboxFetchPipeline(tenantId, delivererKey, rpcClient);
    }

    @AfterMethod
    @SneakyThrows
    public void teardown() {
        closeable.close();
    }

    @Test
    public void fetch() {
        when(rpcClient.invoke(eq(tenantId), any(), any(CommitRequest.class), eq(InboxServiceGrpc.getCommitMethod())))
            .thenReturn(CompletableFuture.completedFuture(CommitReply.newBuilder().build()));
        Fetched fetched = Fetched.newBuilder()
            .setResult(Result.OK)
            .addQos0Msg(InboxMessage.newBuilder().setSeq(1L).build())
            .build();
        InboxReader inboxReader = new InboxReader(inboxId, incarnation, fetchPipeline);
        inboxReader.hint(10);

        ArgumentCaptor<InboxFetchHint> hintCaptor = ArgumentCaptor.forClass(InboxFetchHint.class);
        verify(messageStream, times(1)).ack(hintCaptor.capture());
        InboxFetchHint hint = hintCaptor.getValue();
        inboxReader.fetch(onFetched);
        InboxFetched inboxFetched = InboxFetched.newBuilder()
            .setSessionId(hint.getSessionId())
            .setInboxId(inboxId)
            .setIncarnation(incarnation)
            .setFetched(fetched).build();

        ArgumentCaptor<Consumer<InboxFetched>> messageConsumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(messageStream).onMessage(messageConsumerCaptor.capture());
        messageConsumerCaptor.getValue().accept(inboxFetched);
        verify(onFetched, times(1)).accept(fetched);
    }

    @Test
    public void reFetchAfterRetarget() {
        Fetched fetchedRetry = Fetched.newBuilder().setResult(Result.TRY_LATER).build();
        InboxReader inboxReader = new InboxReader(inboxId, incarnation, fetchPipeline);
        inboxReader.fetch(onFetched);

        ArgumentCaptor<Consumer<Long>> retargetConsumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(messageStream).onRetarget(retargetConsumerCaptor.capture());
        retargetConsumerCaptor.getValue().accept(System.nanoTime());

        verify(onFetched, times(1)).accept(fetchedRetry);
    }

    @Test
    public void registerGC() throws InterruptedException {
        InboxReader inboxReader = new InboxReader(inboxId, incarnation,
            new InboxFetchPipeline(tenantId, delivererKey, rpcClient));
        inboxReader.close();
        inboxReader = null;
        System.gc();
        Thread.sleep(10);
        await().until(() -> {
            verify(messageStream, times(1)).close();
            return true;
        });
    }

    @Test
    public void fetchPipelineClose() throws InterruptedException {
        InboxFetchPipeline inboxFetchPipeline = new InboxFetchPipeline(tenantId, delivererKey, rpcClient);
        inboxFetchPipeline.close();
        Thread.sleep(10);
        await().until(() -> {
            verify(messageStream, times(1)).close();
            return true;
        });
    }
}
