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

import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.baseenv.MemUsage;
import org.apache.bifromq.baserpc.server.ResponsePipeline;
import org.apache.bifromq.inbox.record.TenantInboxInstance;
import org.apache.bifromq.inbox.rpc.proto.SendReply;
import org.apache.bifromq.inbox.rpc.proto.SendRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.sysprops.props.IngressSlowDownDirectMemoryUsage;
import org.apache.bifromq.sysprops.props.IngressSlowDownHeapMemoryUsage;
import org.apache.bifromq.sysprops.props.MaxSlowDownTimeoutSeconds;

@Slf4j
class InboxWriterPipeline extends ResponsePipeline<SendRequest, SendReply> {
    private static final double SLOWDOWN_DIRECT_MEM_USAGE = IngressSlowDownDirectMemoryUsage.INSTANCE.get();
    private static final double SLOWDOWN_HEAP_MEM_USAGE = IngressSlowDownHeapMemoryUsage.INSTANCE.get();
    private static final Duration SLOWDOWN_TIMEOUT = Duration.ofSeconds(MaxSlowDownTimeoutSeconds.INSTANCE.get());
    private final IWriteCallback writeCallback;
    private final ISendRequestHandler handler;
    private final String delivererKey;
    private final ConcurrentLinkedQueue<WriteTask> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean draining = new AtomicBoolean(false);

    public InboxWriterPipeline(IWriteCallback writeCallback,
                               ISendRequestHandler handler,
                               StreamObserver<SendReply> responseObserver) {
        super(responseObserver, () -> MemUsage.local().nettyDirectMemoryUsage() > SLOWDOWN_DIRECT_MEM_USAGE
            || MemUsage.local().heapMemoryUsage() > SLOWDOWN_HEAP_MEM_USAGE, SLOWDOWN_TIMEOUT);
        this.writeCallback = writeCallback;
        this.handler = handler;
        this.delivererKey = metadata(PIPELINE_ATTR_KEY_DELIVERERKEY);
    }

    @Override
    protected CompletableFuture<SendReply> handleRequest(String ignore, SendRequest request) {
        log.trace("Received inbox write request: deliverer={}, \n{}", delivererKey, request);
        return submitAndTrigger(request);
    }

    private CompletableFuture<SendReply> submitAndTrigger(SendRequest request) {
        WriteTask task = new WriteTask(request);
        tasks.add(task);
        task.replyFuture.whenComplete((v, e) -> drain());
        bridge(doWrite(task.request), task.replyFuture);
        return task.onDone;
    }

    private void drain() {
        // Drain in FIFO order without recursion
        while (true) {
            if (!draining.compareAndSet(false, true)) {
                return;
            }
            try {
                while (true) {
                    WriteTask head = tasks.peek();
                    if (head == null) {
                        // nothing to emit
                        break;
                    }
                    if (!head.replyFuture.isDone()) {
                        // head not ready, wait for its completion callback to re-trigger drain
                        break;
                    }
                    // head is ready, emit and remove
                    tasks.poll();
                    bridge(head.replyFuture, head.onDone);
                }
            } finally {
                draining.set(false);
            }
            // loop to check again in case new head becomes ready right after releasing the flag
            WriteTask head = tasks.peek();
            if (head == null || !head.replyFuture.isDone()) {
                return;
            }
        }
    }

    private void bridge(CompletableFuture<SendReply> from, CompletableFuture<SendReply> to) {
        from.whenComplete((v, e) -> {
            if (e != null) {
                to.completeExceptionally(e);
            } else {
                to.complete(v);
            }
        });
    }

    private CompletableFuture<SendReply> doWrite(SendRequest request) {
        return handler.handle(request)
            .thenApply(v -> {
                FetchSignalSender.INSTANCE.execute(() -> {
                    long now = System.nanoTime();
                    v.getReply().getResultMap()
                        .forEach((tenantId, deliveryResults) ->
                            deliveryResults.getResultList()
                                .forEach(result -> {
                                    if (result.getCode() == DeliveryResult.Code.OK) {
                                        writeCallback.afterWrite(
                                            TenantInboxInstance.from(tenantId, result.getMatchInfo()),
                                            delivererKey, now);
                                    }
                                }));
                });
                return v;
            });
    }

    interface IWriteCallback {
        void afterWrite(TenantInboxInstance tenantInboxInstance, String delivererKey, long now);
    }

    interface ISendRequestHandler {
        CompletableFuture<SendReply> handle(SendRequest request);
    }

    private static class WriteTask {
        final SendRequest request;
        final CompletableFuture<SendReply> replyFuture;
        final CompletableFuture<SendReply> onDone;

        WriteTask(SendRequest request) {
            this.request = request;
            this.replyFuture = new CompletableFuture<>();
            this.onDone = new CompletableFuture<>();
        }
    }
}
