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

package org.apache.bifromq.basekv.raft;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import org.apache.bifromq.basekv.raft.proto.Snapshot;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class RaftNodeTest {
    @Mock
    private IRaftNode.IRaftMessageSender messageSender;
    @Mock
    private IRaftNode.IRaftEventListener eventListener;
    @Mock
    private IRaftNode.ISnapshotInstaller snapshotInstaller;
    private RaftNode testNode;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        Snapshot snapshot = Snapshot.newBuilder()
            .setIndex(0)
            .setTerm(0)
            .setClusterConfig(ClusterConfig.newBuilder()
                .addVoters("V1")
                .build()).build();
        IRaftStateStore stateStorage = new InMemoryStateStore("V1", snapshot);
        testNode = new RaftNode(new RaftConfig(), stateStorage, Executors.defaultThreadFactory());
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void id() {
        assertEquals(testNode.id(), "V1");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void currentStateBeforeStart() {
        testNode.status();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void latestConfigBeforeStart() {
        testNode.latestClusterConfig();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void latestSnapshotBeforeStart() {
        testNode.latestSnapshot();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void stepDownBeforeStart() {
        testNode.stepDown();
    }

    @Test
    public void proposeBeforeStart() {
        throwIllegalState(() -> testNode.propose(ByteString.copyFromUtf8("hello")));
    }

    @Test
    public void readIndexBeforeStart() {
        throwIllegalState(() -> testNode.readIndex());
    }

    @Test
    public void compactBeforeStart() {
        throwIllegalState(() -> testNode.compact(ByteString.EMPTY, 0));
    }

    @Test
    public void transferLeadershipBeforeStart() {
        throwIllegalState(() -> testNode.transferLeadership("V2"));
    }

    @Test
    public void recoverBeforeStart() {
        throwIllegalState(() -> testNode.recover());
    }

    @Test
    public void changeClusterConfigBeforeStart() {
        throwIllegalState(
            () -> testNode.changeClusterConfig("abc", Collections.emptySet(), Collections.emptySet()));
    }

    @Test
    public void retrieveCommittedBeforeStart() {
        throwIllegalState(() -> testNode.retrieveCommitted(0, 1024));
    }

    @Test
    public void proposeAfterStop() {
        illegalStateAfterStop(() -> testNode.propose(ByteString.copyFromUtf8("hello")));
    }

    @Test
    public void readIndexAfterStop() {
        illegalStateAfterStop(() -> testNode.readIndex());
    }

    @Test
    public void compactAfterStop() {
        illegalStateAfterStop(() -> testNode.compact(ByteString.EMPTY, 0));
    }

    @Test
    public void transferLeadershipAfterStop() {
        illegalStateAfterStop(() -> testNode.transferLeadership("V2"));
    }

    @Test
    public void recoverAfterStop() {
        illegalStateAfterStop(() -> testNode.recover());
    }

    @Test
    public void changeClusterConfigAfterStop() {
        illegalStateAfterStop(
            () -> testNode.changeClusterConfig("abc", Collections.emptySet(), Collections.emptySet()));
    }

    @Test
    public void retrieveCommittedAfterStop() {
        illegalStateAfterStop(() -> testNode.retrieveCommitted(0, 1024));
    }

    @Test
    public void stopBeforeStart() {
        throwIllegalState(() -> testNode.stop());
    }

    @Test
    public void stop() {
        assertFalse(testNode.isStarted());
        testNode.start(messageSender, eventListener, snapshotInstaller);
        assertTrue(testNode.isStarted());
        testNode.stop().join();
        assertFalse(testNode.isStarted());

        // nothing should happen
        testNode.stop().join();
    }

    @Test
    public void unableToCancelStop() {
        testNode.start(messageSender, eventListener, snapshotInstaller);
        testNode.stop().cancel(true);
        await().until(() -> !testNode.isStarted());
    }

    private <T> void illegalStateAfterStop(Supplier<CompletableFuture<T>> test) {
        testNode.start(messageSender, eventListener, snapshotInstaller);
        testNode.stop();
        throwIllegalState(test);
    }

    private <T> void throwIllegalState(Supplier<CompletableFuture<T>> test) {
        try {
            test.get().join();
        } catch (Throwable t) {
            assertTrue(t.getCause() instanceof IllegalStateException);
        }
    }
}
