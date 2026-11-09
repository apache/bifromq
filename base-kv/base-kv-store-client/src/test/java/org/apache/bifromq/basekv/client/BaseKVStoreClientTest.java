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

package org.apache.bifromq.basekv.client;

import static org.apache.bifromq.basekv.RPCBluePrint.toScopedFullMethodName;
import static org.apache.bifromq.basekv.RPCServerMetadataUtil.RPC_METADATA_STORE_ID;
import static org.apache.bifromq.basekv.raft.proto.RaftNodeStatus.Follower;
import static org.apache.bifromq.basekv.raft.proto.RaftNodeStatus.Leader;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getExecuteMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getLinearizedQueryMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getQueryMethod;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.inprocess.InProcessSocketAddress;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bifromq.basekv.RPCBluePrint;
import org.apache.bifromq.basekv.metaservice.IBaseKVLandscapeObserver;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.raft.proto.RaftNodeStatus;
import org.apache.bifromq.basekv.store.proto.KVRangeROReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRORequest;
import org.apache.bifromq.basekv.store.proto.KVRangeRWReply;
import org.apache.bifromq.basekv.store.proto.KVRangeRWRequest;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceLandscape;
import org.apache.bifromq.baserpc.trafficgovernor.IRPCServiceTrafficService;
import org.apache.bifromq.baserpc.trafficgovernor.ServerEndpoint;
import org.testng.annotations.Test;

public class BaseKVStoreClientTest {
    private static final String CLUSTER_ID = "test-cluster";
    private static final String STORE_A = "store-a";
    private static final String STORE_B = "store-b";
    private static final ByteString SPLIT_KEY = ByteString.copyFromUtf8("a");
    private static final KVRangeId RANGE_1 = KVRangeId.newBuilder().setEpoch(1).setId(1).build();
    private static final KVRangeId RANGE_2 = KVRangeId.newBuilder().setEpoch(1).setId(2).build();

    @Test
    public void closesMutationPipelineWhenRangeLeadershipMoves() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.landscape.onNext(Map.of(
                STORE_A, store(STORE_A, 1, Leader, Leader),
                STORE_B, store(STORE_B, 1, Follower, Follower)));
            fixture.execute(STORE_A, RANGE_1);
            fixture.execute(STORE_A, RANGE_2);
            assertEquals(fixture.first.activeMutations.get(), 2);

            fixture.landscape.onNext(Map.of(
                STORE_A, store(STORE_A, 2, Leader, Follower),
                STORE_B, store(STORE_B, 2, Follower, Leader)));

            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                assertEquals(fixture.first.activeMutations.get(), 1);
                assertEquals(fixture.second.activeMutations.get(), 1);
            });
            fixture.execute(STORE_A, RANGE_1);
            fixture.execute(STORE_B, RANGE_2);
            assertEquals(fixture.first.openedMutations.get(), 2);
            assertEquals(fixture.second.openedMutations.get(), 1);
        }
    }

    @Test
    public void closesMutationPipelinesWhenStoreLosesAllLeaders() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.landscape.onNext(Map.of(
                STORE_A, store(STORE_A, 1, Leader, Follower),
                STORE_B, store(STORE_B, 1, Follower, Leader)));
            fixture.execute(STORE_A, RANGE_1);
            fixture.execute(STORE_B, RANGE_2);

            fixture.landscape.onNext(Map.of(
                STORE_A, store(STORE_A, 2, Follower, Follower),
                STORE_B, store(STORE_B, 2, Leader, Leader)));
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                assertEquals(fixture.first.activeMutations.get(), 0);
                assertEquals(fixture.second.activeMutations.get(), 2);
            });

            fixture.landscape.onNext(Map.of(
                STORE_A, store(STORE_A, 3, Leader, Follower),
                STORE_B, store(STORE_B, 3, Follower, Leader)));
            fixture.execute(STORE_A, RANGE_1);
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                assertEquals(fixture.second.activeMutations.get(), 1));
            fixture.execute(STORE_B, RANGE_2);
            assertEquals(fixture.second.activeMutations.get(), 1);
            assertEquals(fixture.second.openedMutations.get(), 2);
            assertEquals(fixture.first.openedMutations.get(), 2);
        }
    }

    private static KVRangeStoreDescriptor store(String id, long version,
                                                RaftNodeStatus firstRole, RaftNodeStatus secondRole) {
        return KVRangeStoreDescriptor.newBuilder().setId(id).setHlc(version)
            .addRanges(range(RANGE_1, version, firstRole))
            .addRanges(range(RANGE_2, version, secondRole))
            .build();
    }

    private static KVRangeDescriptor range(KVRangeId id, long version, RaftNodeStatus role) {
        return KVRangeDescriptor.newBuilder().setId(id).setVer(version).setHlc(version).setRole(role)
            .setState(State.StateType.Normal)
            .setBoundary(id.equals(RANGE_1) ? toBoundary(null, SPLIT_KEY) : toBoundary(SPLIT_KEY, null))
            .build();
    }

    private static final class Fixture implements AutoCloseable {
        private final Store first = new Store(STORE_A);
        private final Store second = new Store(STORE_B);
        private final BehaviorSubject<Set<ServerEndpoint>> endpoints =
            BehaviorSubject.createDefault(Set.of(first.endpoint(), second.endpoint()));
        private final BehaviorSubject<Map<String, KVRangeStoreDescriptor>> landscape =
            BehaviorSubject.createDefault(Map.of());
        private final IBaseKVStoreClient client;

        private Fixture() throws IOException {
            IBaseKVLandscapeObserver observer = mock(IBaseKVLandscapeObserver.class);
            when(observer.landscape()).thenReturn(landscape);
            IBaseKVMetaService metaService = mock(IBaseKVMetaService.class);
            when(metaService.landscapeObserver(CLUSTER_ID)).thenReturn(observer);
            IRPCServiceLandscape rpcLandscape = mock(IRPCServiceLandscape.class);
            when(rpcLandscape.serverEndpoints()).thenReturn(endpoints);
            when(rpcLandscape.trafficRules()).thenReturn(Observable.just(Map.of()));
            IRPCServiceTrafficService trafficService = mock(IRPCServiceTrafficService.class);
            when(trafficService.getServiceLandscape(RPCBluePrint.build(CLUSTER_ID).serviceDescriptor().getName()))
                .thenReturn(rpcLandscape);
            client = IBaseKVStoreClient.newBuilder().clusterId(CLUSTER_ID).metaService(metaService)
                .trafficService(trafficService).queryPipelinesPerStore(1).build();
        }

        private void execute(String storeId, KVRangeId rangeId) {
            await().atMost(Duration.ofSeconds(3)).ignoreException(ExecutionException.class).untilAsserted(() ->
                assertEquals(client.execute(storeId, KVRangeRWRequest.newBuilder()
                        .setReqId(1).setKvRangeId(rangeId).build())
                    .get(1, TimeUnit.SECONDS).getCode(), ReplyCode.Ok));
        }

        @Override
        public void close() throws InterruptedException {
            try {
                client.close();
            } finally {
                endpoints.onComplete();
                landscape.onComplete();
                first.close();
                second.close();
            }
        }
    }

    private static final class Store implements AutoCloseable {
        private final String id;
        private final String serverName;
        private final Server server;
        private final AtomicInteger activeMutations = new AtomicInteger();
        private final AtomicInteger openedMutations = new AtomicInteger();

        private Store(String id) throws IOException {
            this.id = id;
            BluePrint bluePrint = RPCBluePrint.build(CLUSTER_ID);
            MethodDescriptor<KVRangeRWRequest, KVRangeRWReply> executeMethod = bluePrint.methodDesc(
                toScopedFullMethodName(CLUSTER_ID, getExecuteMethod().getFullMethodName()));
            ServerServiceDefinition.Builder service = ServerServiceDefinition.builder(executeMethod.getServiceName())
                .addMethod(executeMethod, ServerCalls.asyncBidiStreamingCall(this::mutationObserver));
            for (MethodDescriptor<KVRangeRORequest, KVRangeROReply> method :
                List.of(getQueryMethod(), getLinearizedQueryMethod())) {
                service.addMethod(bluePrint.<KVRangeRORequest, KVRangeROReply>methodDesc(
                    toScopedFullMethodName(CLUSTER_ID, method.getFullMethodName())),
                    ServerCalls.asyncBidiStreamingCall(Store::queryObserver));
            }
            serverName = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(service.build()).build().start();
        }

        private ServerEndpoint endpoint() {
            return new ServerEndpoint(ByteString.copyFromUtf8(id), id, "inproc", 0,
                new InProcessSocketAddress(serverName), Set.of(), Map.of(RPC_METADATA_STORE_ID, id), true);
        }

        private StreamObserver<KVRangeRWRequest> mutationObserver(StreamObserver<KVRangeRWReply> responder) {
            activeMutations.incrementAndGet();
            openedMutations.incrementAndGet();
            return new StreamObserver<>() {
                @Override
                public void onNext(KVRangeRWRequest request) {
                    responder.onNext(KVRangeRWReply.newBuilder().setReqId(request.getReqId())
                        .setCode(ReplyCode.Ok).build());
                }

                @Override
                public void onError(Throwable error) {
                    activeMutations.decrementAndGet();
                }

                @Override
                public void onCompleted() {
                    activeMutations.decrementAndGet();
                    responder.onCompleted();
                }
            };
        }

        private static StreamObserver<KVRangeRORequest> queryObserver(StreamObserver<KVRangeROReply> responder) {
            return new StreamObserver<>() {
                @Override
                public void onNext(KVRangeRORequest request) {
                    responder.onNext(KVRangeROReply.newBuilder().setReqId(request.getReqId())
                        .setCode(ReplyCode.Ok).build());
                }

                @Override
                public void onError(Throwable error) {
                }

                @Override
                public void onCompleted() {
                    responder.onCompleted();
                }
            };
        }

        @Override
        public void close() throws InterruptedException {
            server.shutdownNow();
            server.awaitTermination(1, TimeUnit.SECONDS);
        }
    }
}
