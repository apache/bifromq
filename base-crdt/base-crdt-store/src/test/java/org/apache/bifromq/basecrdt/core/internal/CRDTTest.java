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

package org.apache.bifromq.basecrdt.core.internal;

import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.basecrdt.proto.Replacement;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

abstract class CRDTTest {
    protected ScheduledExecutorService executor;

    @BeforeMethod
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod
    public void tearDown() {
        MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
    }

    protected IReplicaStateLattice newStateLattice(Replica ownerReplica, long historyDurationInMS) {
        return new InMemReplicaStateLattice("storeId", ownerReplica,
            Duration.ofMillis(historyDurationInMS),
            Duration.ofMillis(200));
    }

    protected void sync(CausalCRDTInflater<?, ?, ?> left, CausalCRDTInflater<?, ?, ?> right) {
        CompletableFuture<Optional<Iterable<Replacement>>> deltaProto =
            left.delta(right.latticeEvents(), right.historyEvents(), 1024);
        if (deltaProto.join().isPresent()) {
            right.join(deltaProto.join().get()).join();
        }
        deltaProto = right.delta(left.latticeEvents(), left.historyEvents(), 1024);
        if (deltaProto.join().isPresent()) {
            left.join(deltaProto.join().get()).join();
        }
    }
}
