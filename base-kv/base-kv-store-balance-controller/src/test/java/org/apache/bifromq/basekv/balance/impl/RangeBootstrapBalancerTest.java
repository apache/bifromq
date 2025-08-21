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

package org.apache.bifromq.basekv.balance.impl;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.FULL_BOUNDARY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.balance.BalanceNow;
import org.apache.bifromq.basekv.balance.BalanceResult;
import org.apache.bifromq.basekv.balance.BalanceResultType;
import org.apache.bifromq.basekv.balance.command.BootstrapCommand;
import org.apache.bifromq.basekv.proto.KVRangeStoreDescriptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RangeBootstrapBalancerTest {

    private RangeBootstrapBalancer balancer;
    private String clusterId = "testCluster";
    private String localStoreId = "localStore";
    private AtomicLong mockTime;

    @BeforeMethod
    public void setUp() {
        mockTime = new AtomicLong(0L); // Start time at 0
        Supplier<Long> mockMillisSource = mockTime::get;
        balancer = new RangeBootstrapBalancer(clusterId, localStoreId, Duration.ofSeconds(1), mockMillisSource);
    }

    @Test
    public void updateWithoutStoreDescriptors() {
        // Test when there are no store descriptors
        balancer.update(Collections.emptySet());
        mockTime.addAndGet(2000L); // Advance time by 2 seconds

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        assertEquals(((BootstrapCommand) ((BalanceNow<?>) result).command).getBoundary(), FULL_BOUNDARY);
    }


    @Test
    public void balanceWithTrigger() {
        // Test when balance should trigger a bootstrap command
        KVRangeStoreDescriptor storeDescriptor = KVRangeStoreDescriptor.newBuilder()
            .setId(localStoreId)
            .build();

        balancer.update(Set.of(storeDescriptor));
        mockTime.addAndGet(2000L); // Advance time by 2 seconds

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.BalanceNow);
        assertEquals(((BootstrapCommand) ((BalanceNow<?>) result).command).getBoundary(), FULL_BOUNDARY);
    }

    @Test
    public void returnsAwaitImmediatelyBeforeDeadline() {
        balancer.update(Collections.emptySet());

        BalanceResult result = balancer.balance();
        assertSame(result.type(), BalanceResultType.AwaitBalance);

        Duration remaining = ((org.apache.bifromq.basekv.balance.AwaitBalance) result).await;
        assertFalse(remaining.isZero());
        assertTrue(remaining.toMillis() <= 2000L);
    }

    @Test
    public void awaitThenBalanceNowAfterDeadline() {
        balancer.update(Collections.emptySet());

        BalanceResult r1 = balancer.balance();
        assertSame(r1.type(), BalanceResultType.AwaitBalance);
        long r1ms = ((org.apache.bifromq.basekv.balance.AwaitBalance) r1).await.toMillis();
        assertTrue(r1ms > 0);

        long half = Math.max(1, r1ms / 2);
        mockTime.addAndGet(half);
        BalanceResult r2 = balancer.balance();
        assertSame(r2.type(), BalanceResultType.AwaitBalance);
        long r2ms = ((org.apache.bifromq.basekv.balance.AwaitBalance) r2).await.toMillis();
        assertTrue(r2ms >= 0 && r2ms < r1ms);

        mockTime.addAndGet(r2ms + 1);
        BalanceResult r3 = balancer.balance();
        assertSame(r3.type(), BalanceResultType.BalanceNow);
        assertEquals(((BootstrapCommand) ((BalanceNow<?>) r3).command).getBoundary(), FULL_BOUNDARY);
    }

    @Test
    public void noSecondTriggerAfterBootstrapFires() {
        balancer.update(Collections.emptySet());
        mockTime.addAndGet(2000L);
        BalanceResult fired = balancer.balance();
        assertSame(fired.type(), BalanceResultType.BalanceNow);

        BalanceResult next = balancer.balance();
        assertSame(next.type(), BalanceResultType.NoNeedBalance);
    }
}