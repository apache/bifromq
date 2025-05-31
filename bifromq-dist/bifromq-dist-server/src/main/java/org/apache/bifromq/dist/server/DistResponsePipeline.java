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

package org.apache.bifromq.dist.server;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.plugin.eventcollector.distservice.DistError.DistErrorCode.DROP_EXCEED_LIMIT;
import static org.apache.bifromq.plugin.eventcollector.distservice.DistError.DistErrorCode.RPC_FAILURE;

import org.apache.bifromq.baseenv.MemUsage;
import org.apache.bifromq.baserpc.server.ResponsePipeline;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.dist.rpc.proto.DistReply;
import org.apache.bifromq.dist.rpc.proto.DistRequest;
import org.apache.bifromq.dist.server.scheduler.DistServerCallResult;
import org.apache.bifromq.dist.server.scheduler.IDistWorkerCallScheduler;
import org.apache.bifromq.dist.server.scheduler.TenantPubRequest;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.distservice.DistError;
import org.apache.bifromq.plugin.eventcollector.distservice.Disted;
import org.apache.bifromq.sysprops.props.DistWorkerCallQueueNum;
import org.apache.bifromq.sysprops.props.IngressSlowDownDirectMemoryUsage;
import org.apache.bifromq.sysprops.props.IngressSlowDownHeapMemoryUsage;
import org.apache.bifromq.sysprops.props.MaxSlowDownTimeoutSeconds;
import org.apache.bifromq.type.PublisherMessagePack;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DistResponsePipeline extends ResponsePipeline<DistRequest, DistReply> {
    private static final double SLOWDOWN_DIRECT_MEM_USAGE = IngressSlowDownDirectMemoryUsage.INSTANCE.get();
    private static final double SLOWDOWN_HEAP_MEM_USAGE = IngressSlowDownHeapMemoryUsage.INSTANCE.get();
    private static final Duration SLOWDOWN_TIMEOUT = Duration.ofSeconds(MaxSlowDownTimeoutSeconds.INSTANCE.get());
    private final int callQueueIdx = DistQueueAllocator.allocate();
    private final IEventCollector eventCollector;
    private final IDistWorkerCallScheduler distCallScheduler;

    DistResponsePipeline(IDistWorkerCallScheduler distCallScheduler,
                         StreamObserver<DistReply> responseObserver,
                         IEventCollector eventCollector) {
        super(responseObserver, () -> MemUsage.local().nettyDirectMemoryUsage() > SLOWDOWN_DIRECT_MEM_USAGE
            || MemUsage.local().heapMemoryUsage() > SLOWDOWN_HEAP_MEM_USAGE, SLOWDOWN_TIMEOUT);
        this.distCallScheduler = distCallScheduler;
        this.eventCollector = eventCollector;
    }

    @Override
    protected CompletableFuture<DistReply> handleRequest(String tenantId, DistRequest request) {
        return distCallScheduler.schedule(new TenantPubRequest(tenantId, request.getMessagesList(), callQueueIdx))
            .handle(unwrap((result, e) -> {
                DistReply.Builder replyBuilder = DistReply.newBuilder().setReqId(request.getReqId());
                if (e != null) {
                    if (e instanceof BackPressureException) {
                        replyBuilder.setCode(DistReply.Code.BACK_PRESSURE_REJECTED);
                        eventCollector.report(getLocal(DistError.class)
                            .reqId(request.getReqId())
                            .messages(request.getMessagesList())
                            .code(DROP_EXCEED_LIMIT));
                    } else {
                        replyBuilder.setCode(DistReply.Code.ERROR);
                        log.debug("Dist worker call failed", e);
                        eventCollector.report(getLocal(DistError.class)
                            .reqId(request.getReqId())
                            .messages(request.getMessagesList())
                            .code(RPC_FAILURE));
                    }
                } else {
                    switch (result.code()) {
                        case OK -> {
                            int totalFanout = 0;
                            for (PublisherMessagePack publisherMsgPack : request.getMessagesList()) {
                                DistReply.DistResult.Builder resultBuilder = DistReply.DistResult.newBuilder();
                                for (PublisherMessagePack.TopicPack topicPack : publisherMsgPack.getMessagePackList()) {
                                    Integer fanOut = result.fanOut().get(topicPack.getTopic());
                                    if (fanOut == null) {
                                        log.warn("Illegal state: no result for topic: {}", topicPack.getTopic());
                                        resultBuilder.putTopic(topicPack.getTopic(), 0);
                                    } else {
                                        resultBuilder.putTopic(topicPack.getTopic(), fanOut);
                                        totalFanout += fanOut;
                                    }
                                }
                                replyBuilder.setCode(DistReply.Code.OK).addResults(resultBuilder.build());
                            }
                            eventCollector.report(getLocal(Disted.class)
                                .reqId(request.getReqId())
                                .messages(request.getMessagesList())
                                .fanout(totalFanout));
                        }
                        case TryLater -> replyBuilder.setCode(DistReply.Code.TRY_LATER);
                        default -> {
                            assert result.code() == DistServerCallResult.Code.Error;
                            replyBuilder.setCode(DistReply.Code.ERROR);
                        }
                    }
                }
                return replyBuilder.build();
            }));
    }

    private static class DistQueueAllocator {
        private static final int QUEUE_NUMS = DistWorkerCallQueueNum.INSTANCE.get();
        private static final AtomicInteger IDX = new AtomicInteger(0);

        public static int allocate() {
            return IDX.getAndIncrement() % QUEUE_NUMS;
        }
    }
}
