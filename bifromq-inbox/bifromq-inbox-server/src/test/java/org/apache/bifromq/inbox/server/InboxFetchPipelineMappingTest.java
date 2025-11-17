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

package org.apache.bifromq.inbox.server;

import static org.apache.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_DELIVERERKEY;
import static org.apache.bifromq.inbox.util.PipelineUtil.PIPELINE_ATTR_KEY_ID;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.SneakyThrows;
import org.apache.bifromq.baserpc.RPCContext;
import org.apache.bifromq.baserpc.metrics.IRPCMeter;
import org.apache.bifromq.baserpc.metrics.RPCMetric;
import org.apache.bifromq.inbox.rpc.proto.InboxFetchHint;
import org.apache.bifromq.inbox.rpc.proto.InboxFetched;
import org.apache.bifromq.inbox.server.scheduler.FetchRequest;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.inbox.storage.proto.InboxMessage;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxFetchPipelineMappingTest {
    private static final String TENANT = "tenantA";
    private static final String DELIVERER = "delivererA";
    private static final String INBOX = "inboxA";
    private static final long INCARNATION = 1L;
    private final List<InboxFetched> received = new ArrayList<>();
    private AutoCloseable closeable;
    @Mock
    private ServerCallStreamObserver<InboxFetched> responseObserver;

    private static InboxFetchHint hint(long sessionId, int capacity) {
        return hint(sessionId, capacity, -1, -1);
    }

    private static InboxFetchHint hint(long sessionId,
                                       int capacity,
                                       long lastFetchQoS0Seq,
                                       long lastFetchSendBufferSeq) {
        return InboxFetchHint.newBuilder()
            .setSessionId(sessionId)
            .setInboxId(INBOX)
            .setIncarnation(INCARNATION)
            .setCapacity(capacity)
            .setLastFetchQoS0Seq(lastFetchQoS0Seq)
            .setLastFetchSendBufferSeq(lastFetchSendBufferSeq)
            .build();
    }

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        setupContext();
        doAnswer((Answer<Void>) invocation -> {
            InboxFetched v = invocation.getArgument(0);
            synchronized (received) {
                received.add(v);
            }
            return null;
        }).when(responseObserver).onNext(any());
    }

    private void setupContext() {
        Map<String, String> meta = new HashMap<>();
        meta.put(PIPELINE_ATTR_KEY_ID, "p1");
        meta.put(PIPELINE_ATTR_KEY_DELIVERERKEY, DELIVERER);
        Context.current()
            .withValue(RPCContext.TENANT_ID_CTX_KEY, TENANT)
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, meta)
            .withValue(RPCContext.METER_KEY_CTX_KEY, new IRPCMeter.IRPCMethodMeter() {
                @Override
                public void recordCount(RPCMetric metric) {}

                @Override
                public void recordCount(RPCMetric metric, double inc) {}

                @Override
                public Timer timer(RPCMetric metric) {
                    return Timer.builder("dummy").register(new SimpleMeterRegistry());
                }

                @Override
                public void recordSummary(RPCMetric metric, int depth) {}
            }).attach();
    }

    @SneakyThrows
    @AfterMethod
    public void teardown() {
        closeable.close();
    }

    private InboxFetchPipeline.Fetcher noopFetcher() {
        return req -> CompletableFuture.completedFuture(Fetched.newBuilder()
            .setResult(Fetched.Result.OK)
            .build());
    }

    @Test
    public void closeOneSessionShouldNotRemoveOthers() {
        InboxFetcherRegistry registry = new InboxFetcherRegistry();
        InboxFetchPipeline pipeline = new InboxFetchPipeline(responseObserver, noopFetcher(), registry);

        long sessionA = 1001L;
        long sessionB = 2002L;

        pipeline.onNext(hint(sessionA, 10));
        pipeline.onNext(hint(sessionB, 10));

        await().until(() -> {
            synchronized (received) {
                return received.size() >= 2;
            }
        });

        // close session B (capacity < 0)
        pipeline.onNext(hint(sessionB, -1));

        // signal fetch for the inbox; should still reach session A only
        boolean signalled = pipeline.signalFetch(INBOX, INCARNATION, System.nanoTime());
        assertTrue(signalled);

        await().until(() -> {
            synchronized (received) {
                return received.size() >= 3;
            }
        });
        InboxFetched last = lastReceived();
        assertEquals(last.getSessionId(), sessionA);
    }

    @Test
    public void shouldNotRewindStartAfterWhenHintIsStale() {
        InboxFetcherRegistry registry = new InboxFetcherRegistry();
        TestFetcher fetcher = new TestFetcher();
        InboxFetchPipeline pipeline = new InboxFetchPipeline(responseObserver, fetcher, registry);

        long sessionId = 3003L;
        pipeline.onNext(hint(sessionId, 2, -1, -1));

        FetchRequest firstRequest = fetcher.awaitRequest();
        assertEquals(firstRequest.params().getQos0StartAfter(), -1);
        assertEquals(firstRequest.params().getSendBufferStartAfter(), -1);

        fetcher.completeNext(Fetched.newBuilder()
            .setResult(Fetched.Result.OK)
            .addQos0Msg(InboxMessage.newBuilder().setSeq(5).build())
            .addQos0Msg(InboxMessage.newBuilder().setSeq(6).build())
            .addSendBufferMsg(InboxMessage.newBuilder().setSeq(10).build())
            .addSendBufferMsg(InboxMessage.newBuilder().setSeq(11).build())
            .build());

        await().until(() -> {
            synchronized (received) {
                return !received.isEmpty();
            }
        });

        pipeline.onNext(hint(sessionId, 3, 1, 7));

        FetchRequest secondRequest = fetcher.awaitRequest();
        assertEquals(secondRequest.params().getQos0StartAfter(), 6);
        assertEquals(secondRequest.params().getSendBufferStartAfter(), 11);

        fetcher.completeNext(Fetched.newBuilder()
            .setResult(Fetched.Result.OK)
            .build());

        pipeline.close();
    }

    private InboxFetched lastReceived() {
        synchronized (received) {
            return received.get(received.size() - 1);
        }
    }

    private static class TestFetcher implements InboxFetchPipeline.Fetcher {
        private final BlockingQueue<FetchRequest> requests = new LinkedBlockingQueue<>();
        private final BlockingQueue<CompletableFuture<Fetched>> responses = new LinkedBlockingQueue<>();

        @Override
        public CompletableFuture<Fetched> fetch(FetchRequest request) {
            CompletableFuture<Fetched> future = new CompletableFuture<>();
            requests.add(request);
            responses.add(future);
            return future;
        }

        FetchRequest awaitRequest() {
            await().until(() -> !requests.isEmpty());
            FetchRequest request = requests.poll();
            assertNotNull(request);
            return request;
        }

        void completeNext(Fetched fetched) {
            await().until(() -> !responses.isEmpty());
            CompletableFuture<Fetched> future = responses.poll();
            assertNotNull(future);
            future.complete(fetched);
        }
    }
}
