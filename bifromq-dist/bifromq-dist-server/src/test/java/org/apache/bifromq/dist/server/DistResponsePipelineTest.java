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

package org.apache.bifromq.dist.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.bifromq.baserpc.RPCContext;
import org.apache.bifromq.baserpc.metrics.IRPCMeter;
import org.apache.bifromq.baserpc.metrics.RPCMetric;
import org.apache.bifromq.dist.rpc.proto.DistReply;
import org.apache.bifromq.dist.rpc.proto.DistRequest;
import org.apache.bifromq.dist.server.scheduler.IDistWorkerCallScheduler;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.PublisherMessagePack;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DistResponsePipelineTest {
    private AutoCloseable closeable;

    @Mock
    private IDistWorkerCallScheduler scheduler;

    @Mock
    private ServerCallStreamObserver<DistReply> responseObserver;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        setupContext();
    }

    private void setupContext() {
        Map<String, String> metadata = new HashMap<>();
        Context.current()
            .withValue(RPCContext.METER_KEY_CTX_KEY, createMockMeter())
            .withValue(RPCContext.TENANT_ID_CTX_KEY, "tenantId")
            .withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metadata)
            .attach();
    }

    private IRPCMeter.IRPCMethodMeter createMockMeter() {
        return new IRPCMeter.IRPCMethodMeter() {
            @Override
            public void recordCount(RPCMetric metric) {
            }

            @Override
            public void recordCount(RPCMetric metric, double inc) {
            }

            @Override
            public Timer timer(RPCMetric metric) {
                return Timer.builder("dummy").register(new SimpleMeterRegistry());
            }

            @Override
            public void recordSummary(RPCMetric metric, int depth) {
            }
        };
    }

    @SneakyThrows
    @AfterMethod
    public void tearDown() {
        closeable.close();
    }

    @Test
    public void testEmitOrderWithConcurrentBatches() throws Exception {
        DistRequest r1 = DistRequest.newBuilder()
            .setReqId(1)
            .addMessages(publisher("tenantA", topicPack("t/1"), topicPack("t/2")))
            .build();
        DistRequest r2 = DistRequest.newBuilder()
            .setReqId(2)
            .addMessages(publisher("tenantA", topicPack("t/3")))
            .build();

        CompletableFuture<Map<String, Integer>> f1 = new CompletableFuture<>();
        CompletableFuture<Map<String, Integer>> f2 = new CompletableFuture<>();
        when(scheduler.schedule(any())).thenReturn(f1).thenReturn(f2);

        DistResponsePipeline pipeline = new DistResponsePipeline(scheduler, responseObserver, event -> {});

        CompletableFuture<DistReply> resp1 = pipeline.handleRequest("tenantA", r1);
        CompletableFuture<DistReply> resp2 = pipeline.handleRequest("tenantA", r2);

        Map<String, Integer> m2 = new HashMap<>();
        m2.put("t/3", 1);
        f2.complete(m2);

        assertFalse(resp2.isDone());

        Map<String, Integer> m1 = new HashMap<>();
        m1.put("t/1", 1);
        m1.put("t/2", 2);
        f1.complete(m1);

        DistReply reply1 = resp1.get(3, TimeUnit.SECONDS);
        DistReply reply2 = resp2.get(3, TimeUnit.SECONDS);

        assertEquals(reply1.getReqId(), 1L);
        assertEquals(reply2.getReqId(), 2L);
        assertEquals(reply1.getResultsCount(), r1.getMessagesCount());
        assertEquals(reply2.getResultsCount(), r2.getMessagesCount());
    }

    private PublisherMessagePack.TopicPack topicPack(String topic) {
        return PublisherMessagePack.TopicPack.newBuilder().setTopic(topic).build();
    }

    private PublisherMessagePack publisher(String tenantId, PublisherMessagePack.TopicPack... packs) {
        PublisherMessagePack.Builder b = PublisherMessagePack.newBuilder()
            .setPublisher(ClientInfo.newBuilder().setTenantId(tenantId).build());
        for (PublisherMessagePack.TopicPack p : packs) {
            b.addMessagePack(p);
        }
        return b.build();
    }
}

