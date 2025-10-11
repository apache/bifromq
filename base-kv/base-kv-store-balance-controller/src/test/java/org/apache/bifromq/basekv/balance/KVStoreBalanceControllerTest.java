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

package org.apache.bifromq.basekv.balance;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.balance.command.BootstrapCommand;
import org.apache.bifromq.basekv.balance.command.ChangeConfigCommand;
import org.apache.bifromq.basekv.balance.command.MergeCommand;
import org.apache.bifromq.basekv.balance.command.RecoveryCommand;
import org.apache.bifromq.basekv.balance.command.SplitCommand;
import org.apache.bifromq.basekv.balance.command.TransferLeadershipCommand;
import org.apache.bifromq.basekv.balance.utils.DescriptorUtils;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.metaservice.IBaseKVMetaService;
import org.apache.bifromq.basekv.metaservice.IBaseKVStoreBalancerStatesProposal;
import org.apache.bifromq.basekv.metaservice.IBaseKVStoreBalancerStatesReporter;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.apache.bifromq.basekv.store.proto.KVRangeSplitReply;
import org.apache.bifromq.basekv.store.proto.ReplyCode;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class KVStoreBalanceControllerTest {

    private static final String CLUSTER_ID = "test_cluster";
    private static final String LOCAL_STORE_ID = "localStoreId";
    private final PublishSubject<Map<String, BalancerStateSnapshot>> proposalSubject = PublishSubject.create();
    private final PublishSubject<Set<KVRangeStoreDescriptor>> storeDescSubject = PublishSubject.create();
    private final PublishSubject<Long> refreshSignal = PublishSubject.create();
    @Mock
    private IBaseKVMetaService metaService;
    @Mock
    private IBaseKVStoreBalancerStatesProposal statesProposal;
    @Mock
    private IBaseKVStoreBalancerStatesReporter statesReporter;
    @Mock
    private IBaseKVStoreClient storeClient;
    @Mock
    private IStoreBalancerFactory balancerFactory;
    @Mock
    private StoreBalancer storeBalancer;
    private ScheduledExecutorService executor;
    private KVStoreBalanceController balanceController;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() throws IOException {
        closeable = MockitoAnnotations.openMocks(this);
        when(storeClient.clusterId()).thenReturn(CLUSTER_ID);
        when(storeBalancer.initialLoadRules()).thenReturn(Struct.getDefaultInstance());
        when(balancerFactory.newBalancer(eq(CLUSTER_ID), eq(LOCAL_STORE_ID))).thenReturn(storeBalancer);
        when(metaService.balancerStatesProposal(eq(CLUSTER_ID))).thenReturn(statesProposal);
        when(metaService.balancerStatesReporter(eq(CLUSTER_ID), eq(LOCAL_STORE_ID))).thenReturn(statesReporter);
        when(statesReporter.refreshSignal()).thenReturn(refreshSignal);
        when(statesProposal.expectedBalancerStates()).thenReturn(proposalSubject);
        when(storeClient.describe()).thenReturn(storeDescSubject);
        executor = Executors.newScheduledThreadPool(1);
        balanceController = new KVStoreBalanceController(metaService,
            storeClient,
            List.of(balancerFactory),
            Duration.ofMillis(100),
            Duration.ofMillis(100),
            Duration.ofMillis(100),
            executor);
        balanceController.start(LOCAL_STORE_ID);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        balanceController.stop();
        closeable.close();
    }

    @Test
    public void testNoInput() {
        awaitExecute(200);
        verify(storeBalancer, times(0)).update(any(Struct.class));
        awaitExecute(200);
        verify(storeBalancer, times(0)).update(any(Set.class));
    }

    @Test
    public void testInput() {
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        when(storeBalancer.validate(any())).thenReturn(true);
        when(storeBalancer.balance()).thenReturn(NoNeedBalance.INSTANCE);
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeBalancer, times(1)).update(eq(storeDescriptors));
        awaitExecute(200);
        verify(storeBalancer, times(1)).balance();

        log.info("Test input done");
        BalancerStateSnapshot proposal = BalancerStateSnapshot.newBuilder()
            .setLoadRules(Struct.newBuilder()
                .putFields("test", Value.newBuilder().setNumberValue(1).build())
                .build())
            .build();
        proposalSubject.onNext(Map.of(balancerFactory.getClass().getName(), proposal));
        awaitExecute(200);
        verify(storeBalancer, times(1)).update(eq(proposal.getLoadRules()));
        verify(storeBalancer, times(2)).balance();
    }

    @Test
    public void testBootStrapCommand() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(
            BootstrapCommand.builder().kvRangeId(id).toStore(LOCAL_STORE_ID).boundary(FULL_BOUNDARY).build()));
        when(storeClient.bootstrap(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(1)).bootstrap(eq(LOCAL_STORE_ID),
            argThat(c -> c.getKvRangeId().equals(id) && c.getBoundary().equals(FULL_BOUNDARY)));
    }

    @Test
    public void testChangeConfig() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        ChangeConfigCommand command =
            ChangeConfigCommand.builder().kvRangeId(id).expectedVer(2L).toStore(LOCAL_STORE_ID)
                .voters(Sets.newHashSet(LOCAL_STORE_ID, "store1")).learners(Sets.newHashSet("learner1")).build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.changeReplicaConfig(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(1)).changeReplicaConfig(eq(LOCAL_STORE_ID), argThat(
            r -> r.getKvRangeId().equals(id) && r.getVer() == command.getExpectedVer() &&
                r.getNewVotersList().containsAll(command.getVoters()) &&
                r.getNewLearnersList().containsAll(command.getLearners())));
    }

    @Test
    public void testMerge() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        MergeCommand command = MergeCommand.builder()
            .kvRangeId(id)
            .mergeeId(KVRangeIdUtil.generate())
            .toStore(LOCAL_STORE_ID)
            .voters(Set.of(LOCAL_STORE_ID))
            .expectedVer(2L).build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.mergeRanges(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(1)).mergeRanges(eq(LOCAL_STORE_ID), argThat(
            r -> r.getMergerId().equals(id) && r.getVer() == command.getExpectedVer()
                && r.getMergeeId().equals(command.getMergeeId())));
    }

    @Test
    public void testSplit() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command =
            SplitCommand.builder().toStore(LOCAL_STORE_ID).kvRangeId(id).splitKey(ByteString.copyFromUtf8("splitKey"))
                .expectedVer(2L).build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(1)).splitRange(eq(LOCAL_STORE_ID), argThat(
            r -> r.getKvRangeId().equals(id) && r.getVer() == command.getExpectedVer() &&
                r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testTransferLeadership() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        TransferLeadershipCommand command =
            TransferLeadershipCommand.builder().toStore(LOCAL_STORE_ID).newLeaderStore("store1").kvRangeId(id)
                .expectedVer(2L).build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.transferLeadership(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(1)).transferLeadership(eq(LOCAL_STORE_ID), argThat(
            r -> r.getKvRangeId().equals(id) && r.getVer() == command.getExpectedVer() &&
                r.getNewLeaderStore().equals(command.getNewLeaderStore())));
    }

    @Test
    public void testRecover() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        RecoveryCommand command = RecoveryCommand.builder().toStore(LOCAL_STORE_ID).kvRangeId(id).build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.recover(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(1)).recover(eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)));
    }

    @Test
    public void testRangeCommandRunHistoryKept() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command =
            SplitCommand.builder().toStore(LOCAL_STORE_ID).kvRangeId(id).splitKey(ByteString.copyFromUtf8("splitKey"))
                .expectedVer(2L).build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any())).thenReturn(
            CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder().setCode(ReplyCode.Ok).build()));
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(1)).splitRange(eq(LOCAL_STORE_ID), argThat(
            r -> r.getKvRangeId().equals(id) && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));

        proposalSubject.onNext(Map.of(storeBalancer.getClass().getName(), BalancerStateSnapshot.getDefaultInstance()));

        awaitExecute(200);
        verify(storeClient, times(1)).splitRange(eq(LOCAL_STORE_ID), argThat(
            r -> r.getKvRangeId().equals(id) && r.getVer() == command.getExpectedVer() &&
                r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testRangeCommandRunHistoryCleared() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor(id, 2L);
        SplitCommand command =
            SplitCommand.builder().toStore(LOCAL_STORE_ID).kvRangeId(id).splitKey(ByteString.copyFromUtf8("splitKey"))
                .expectedVer(2L).build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any())).thenReturn(
            CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder().setCode(ReplyCode.Ok).build()));
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(1)).splitRange(eq(LOCAL_STORE_ID), argThat(
            r -> r.getKvRangeId().equals(id) && r.getVer() == command.getExpectedVer() &&
                r.getSplitKey().equals(command.getSplitKey())));

        storeDescriptors = generateDescriptor();
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(2)).splitRange(eq(LOCAL_STORE_ID), argThat(
            r -> r.getKvRangeId().equals(id) && r.getVer() == command.getExpectedVer() &&
                r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testRetryWhenCommandRunFailed() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor(id, 2L);
        SplitCommand command =
            SplitCommand.builder().toStore(LOCAL_STORE_ID).kvRangeId(id).splitKey(ByteString.copyFromUtf8("splitKey"))
                .expectedVer(2L).build();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Mock Exception")),
            CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder().setCode(ReplyCode.Ok).build()));
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(1200);
        verify(storeClient, times(2)).splitRange(eq(LOCAL_STORE_ID), argThat(
            r -> r.getKvRangeId().equals(id) && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testRetryWhenCommandNotReady() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command =
            SplitCommand.builder().toStore(LOCAL_STORE_ID).kvRangeId(id).splitKey(ByteString.copyFromUtf8("splitKey"))
                .expectedVer(2L).build();
        when(storeBalancer.balance()).thenReturn(AwaitBalance.of(Duration.ofMillis(200)), BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any())).thenReturn(
            CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder().setCode(ReplyCode.Ok).build()));
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(1200);
        verify(storeClient, times(1)).splitRange(eq(LOCAL_STORE_ID), argThat(
            r -> r.getKvRangeId().equals(id) && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testRetryBeingPreempted() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        SplitCommand command = SplitCommand.builder()
            .toStore(LOCAL_STORE_ID)
            .kvRangeId(id)
            .splitKey(ByteString.copyFromUtf8("splitKey"))
            .expectedVer(2L).build();
        when(storeBalancer.validate(any())).thenReturn(true);
        when(storeBalancer.balance()).thenReturn(AwaitBalance.of(Duration.ofSeconds(10)), BalanceNow.of(command));
        when(storeClient.splitRange(eq(LOCAL_STORE_ID), any()))
            .thenReturn(CompletableFuture.completedFuture(KVRangeSplitReply.newBuilder()
                .setCode(ReplyCode.Ok)
                .build()));
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeBalancer, times(1)).update(any(Set.class));

        BalancerStateSnapshot proposal = BalancerStateSnapshot.newBuilder()
            .setDisable(false)
            .setLoadRules(Struct.newBuilder()
                .putFields("test", Value.newBuilder().setNumberValue(1).build())
                .build())
            .build();
        proposalSubject.onNext(Map.of(balancerFactory.getClass().getName(), proposal));
        awaitExecute(200);
        verify(storeBalancer, times(1)).update(any(Struct.class));
        awaitExecute(200);
        verify(statesReporter).reportBalancerState(anyString(), eq(false), eq(proposal.getLoadRules()));
        verify(storeClient, times(1)).splitRange(
            eq(LOCAL_STORE_ID),
            argThat(r -> r.getKvRangeId().equals(id)
                && r.getVer() == command.getExpectedVer()
                && r.getSplitKey().equals(command.getSplitKey())));
    }

    @Test
    public void testDisableBalancer() {
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor();
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(
            BootstrapCommand.builder().kvRangeId(id).toStore(LOCAL_STORE_ID).boundary(FULL_BOUNDARY).build()));
        when(storeClient.bootstrap(eq(LOCAL_STORE_ID), any())).thenReturn(new CompletableFuture<>());

        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeClient, times(1)).bootstrap(eq(LOCAL_STORE_ID),
            argThat(c -> c.getKvRangeId().equals(id) && c.getBoundary().equals(FULL_BOUNDARY)));

        proposalSubject.onNext(Map.of(storeBalancer.getClass().getName(),
            BalancerStateSnapshot.newBuilder().setDisable(true).build()));
        reset(storeClient);
        awaitExecute(200);
        verify(storeClient, times(0)).bootstrap(eq(LOCAL_STORE_ID),
            argThat(c -> c.getKvRangeId().equals(id) && c.getBoundary().equals(FULL_BOUNDARY)));
    }

    @Test
    public void testInvalidRules() {
        reset(statesReporter);
        KVRangeId id = KVRangeIdUtil.generate();
        Set<KVRangeStoreDescriptor> storeDescriptors = generateDescriptor(id, 2L);
        when(storeClient.bootstrap(anyString(), any())).thenReturn(new CompletableFuture<>());
        when(storeBalancer.validate(any())).thenReturn(false);
        when(storeBalancer.balance()).thenReturn(BalanceNow.of(
            BootstrapCommand.builder().kvRangeId(id).toStore(LOCAL_STORE_ID).boundary(FULL_BOUNDARY).build()));
        storeDescSubject.onNext(storeDescriptors);
        awaitExecute(200);
        verify(storeBalancer, never()).update(any(Struct.class));
        verify(statesReporter, never()).reportBalancerState(anyString(), anyBoolean(), any(Struct.class));
    }

    @Test
    public void testRefreshSignal() {
        reset(statesReporter);
        refreshSignal.onNext(System.currentTimeMillis());
        verify(statesReporter, times(1))
            .reportBalancerState(anyString(), anyBoolean(), any(Struct.class));
        verify(statesReporter, times(1))
            .reportBalancerState(eq(balancerFactory.getClass().getName()),
                eq(false),
                eq(Struct.getDefaultInstance()));
    }

    private Set<KVRangeStoreDescriptor> generateDescriptor(KVRangeId id, long ver) {
        List<String> voters = Lists.newArrayList(LOCAL_STORE_ID, "store1");
        List<String> learners = Lists.newArrayList();
        List<KVRangeDescriptor> rangeDescriptors =
            DescriptorUtils.generateRangeDesc(id, ver, Sets.newHashSet(voters), Sets.newHashSet(learners));
        Set<KVRangeStoreDescriptor> storeDescriptors = new HashSet<>();
        for (int i = 0; i < voters.size(); i++) {
            storeDescriptors.add(
                KVRangeStoreDescriptor.newBuilder().setId(voters.get(i)).putStatistics("cpu.usage", 0.1)
                    .addRanges(rangeDescriptors.get(i)).build());
        }
        return storeDescriptors;
    }

    private Set<KVRangeStoreDescriptor> generateDescriptor() {
        return generateDescriptor(KVRangeIdUtil.generate(), 0L);
    }

    private void awaitExecute(long delayMS) {
        CompletableFuture<Void> onDone = new CompletableFuture<>();
        executor.schedule(() -> onDone.complete(null), delayMS, TimeUnit.MILLISECONDS);
        onDone.join();
    }
}
