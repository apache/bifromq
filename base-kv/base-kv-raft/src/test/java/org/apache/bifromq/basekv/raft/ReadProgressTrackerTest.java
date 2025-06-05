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

package org.apache.bifromq.basekv.raft;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.bifromq.basekv.raft.exception.ReadIndexException;
import org.apache.bifromq.basekv.raft.proto.ClusterConfig;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReadProgressTrackerTest {
    ReadProgressTracker readProgressTracker;
    @Mock
    IRaftStateStore stateStorage;
    @Mock
    Logger logger;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        readProgressTracker = new ReadProgressTracker(stateStorage, logger);
    }

    @AfterMethod
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void testHighestReadIndexAfterInit() {
        assertEquals(readProgressTracker.highestReadIndex(), 0);
    }

    @Test
    public void testAdd() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());
        CompletableFuture<Long> onDone = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone);
        assertEquals(readProgressTracker.underConfirming(), 1);
        assertFalse(onDone.isDone());

        onDone = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone);
        assertEquals(readProgressTracker.underConfirming(), 2);
        assertFalse(onDone.isDone());

        onDone = new CompletableFuture<>();
        readProgressTracker.add(6L, onDone);
        assertEquals(readProgressTracker.underConfirming(), 3);
        assertFalse(onDone.isDone());
    }

    @Test
    public void testAbort() {
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());

        CompletableFuture<Long> onDone1 = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone1);

        CompletableFuture<Long> onDone2 = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone2);

        CompletableFuture<Long> onDone3 = new CompletableFuture<>();
        readProgressTracker.add(6L, onDone3);
        readProgressTracker.abort(ReadIndexException.leaderStepDown());
        assertEquals(readProgressTracker.underConfirming(), 0);
        assertTrue(onDone1.isCompletedExceptionally());
        assertTrue(onDone2.isCompletedExceptionally());
        assertTrue(onDone3.isCompletedExceptionally());
    }

    @Test
    public void testConfirm() {
        try {
            readProgressTracker.confirm(-1L, "FakePeer");
        } catch (Exception e) {
            fail();
        }
        when(stateStorage.latestClusterConfig()).thenReturn(ClusterConfig.newBuilder()
            .addVoters("V1")
            .addVoters("V2")
            .addVoters("V3")
            .build());

        CompletableFuture<Long> onDone1 = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone1);

        CompletableFuture<Long> onDone2 = new CompletableFuture<>();
        readProgressTracker.add(5L, onDone2);

        CompletableFuture<Long> onDone3 = new CompletableFuture<>();
        readProgressTracker.add(6L, onDone3);

        CompletableFuture<Long> onDone4 = new CompletableFuture<>();
        readProgressTracker.add(7L, onDone4);

        readProgressTracker.confirm(6L, "V1");
        assertFalse(onDone1.isDone());
        assertFalse(onDone2.isDone());
        assertFalse(onDone3.isDone());
        assertFalse(onDone4.isDone());
        readProgressTracker.confirm(6L, "V2");
        assertTrue(onDone1.isDone());
        assertTrue(onDone2.isDone());
        assertTrue(onDone3.isDone());
        assertFalse(onDone4.isDone());
        assertEquals(readProgressTracker.underConfirming(), 1);
        readProgressTracker.confirm(7L, "V2");
        assertFalse(onDone4.isDone());
        readProgressTracker.confirm(7L, "V3");
        assertTrue(onDone4.isDone());
        assertEquals(readProgressTracker.underConfirming(), 0);
    }
}
