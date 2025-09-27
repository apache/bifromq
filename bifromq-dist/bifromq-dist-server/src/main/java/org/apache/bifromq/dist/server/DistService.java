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

import static org.apache.bifromq.baserpc.server.UnaryResponse.response;

import io.grpc.stub.StreamObserver;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.dist.rpc.proto.DistReply;
import org.apache.bifromq.dist.rpc.proto.DistRequest;
import org.apache.bifromq.dist.rpc.proto.DistServiceGrpc;
import org.apache.bifromq.dist.rpc.proto.MatchReply;
import org.apache.bifromq.dist.rpc.proto.MatchRequest;
import org.apache.bifromq.dist.rpc.proto.UnmatchReply;
import org.apache.bifromq.dist.rpc.proto.UnmatchRequest;
import org.apache.bifromq.dist.server.handler.MatchReqHandler;
import org.apache.bifromq.dist.server.handler.UnmatchReqHandler;
import org.apache.bifromq.dist.server.scheduler.BatchDistServerCallBuilderFactory;
import org.apache.bifromq.dist.server.scheduler.DistWorkerCallScheduler;
import org.apache.bifromq.dist.server.scheduler.IDistWorkerCallScheduler;
import org.apache.bifromq.dist.server.scheduler.IMatchCallScheduler;
import org.apache.bifromq.dist.server.scheduler.IUnmatchCallScheduler;
import org.apache.bifromq.dist.server.scheduler.MatchCallScheduler;
import org.apache.bifromq.dist.server.scheduler.UnmatchCallScheduler;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sysprops.props.DistMaxCachedRoutesPerTenant;
import org.apache.bifromq.sysprops.props.DistTopicMatchExpirySeconds;

@Slf4j
public class DistService extends DistServiceGrpc.DistServiceImplBase {
    private final IEventCollector eventCollector;
    private final IDistWorkerCallScheduler distCallScheduler;
    private final MatchReqHandler matchReqHandler;
    private final UnmatchReqHandler unmatchReqHandler;

    DistService(IBaseKVStoreClient distWorkerClient, ISettingProvider settingProvider, IEventCollector eventCollector) {
        this.eventCollector = eventCollector;

        IMatchCallScheduler matchCallScheduler = new MatchCallScheduler(distWorkerClient, settingProvider);
        matchReqHandler = new MatchReqHandler(eventCollector, matchCallScheduler);

        IUnmatchCallScheduler unmatchCallScheduler = new UnmatchCallScheduler(distWorkerClient);
        unmatchReqHandler = new UnmatchReqHandler(eventCollector, unmatchCallScheduler);

        BatchDistServerCallBuilderFactory factory = new BatchDistServerCallBuilderFactory(distWorkerClient,
            DistMaxCachedRoutesPerTenant.INSTANCE.get(),
            Duration.ofSeconds(DistTopicMatchExpirySeconds.INSTANCE.get()));
        this.distCallScheduler = new DistWorkerCallScheduler(factory, Long.MAX_VALUE);
    }

    @Override
    public void match(MatchRequest request, StreamObserver<MatchReply> responseObserver) {
        response(tenantId -> matchReqHandler.handle(request), responseObserver);
    }

    public void unmatch(UnmatchRequest request, StreamObserver<UnmatchReply> responseObserver) {
        response(tenantId -> unmatchReqHandler.handle(request), responseObserver);
    }

    @Override
    public StreamObserver<DistRequest> dist(StreamObserver<DistReply> responseObserver) {
        return new DistResponsePipeline(distCallScheduler, responseObserver, eventCollector);
    }

    public void stop() {
        log.debug("stop dist worker call scheduler");
        distCallScheduler.close();
        log.debug("Stop match call handler");
        matchReqHandler.close();
        log.debug("Stop unmatch call handler");
        unmatchReqHandler.close();
    }
}
