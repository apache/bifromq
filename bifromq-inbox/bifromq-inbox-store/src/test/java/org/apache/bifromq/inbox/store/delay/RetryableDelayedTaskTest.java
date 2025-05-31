/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.inbox.store.delay;

import static org.mockito.Mockito.verify;

import org.apache.bifromq.inbox.record.InboxInstance;
import org.apache.bifromq.inbox.record.TenantInboxInstance;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetryableDelayedTaskTest {

    private TenantInboxInstance tenantInboxInstance;
    @Mock
    private IDelayTaskRunner<TenantInboxInstance> runner;

    private AutoCloseable closeable;

    @BeforeMethod
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        tenantInboxInstance = new TenantInboxInstance("tenant1", new InboxInstance("inbox1", 1L));
    }

    @Test
    public void testRetryCountExceeded() throws Exception {
        TestTask task = new TestTask(Duration.ofMillis(100), 1L, 6, false);
        CompletableFuture<Void> future = task.run(tenantInboxInstance, runner);
        future.get();

        Assert.assertFalse(task.callOperationInvoked);
        verify(runner, Mockito.never()).schedule(Mockito.any(), Mockito.any());
    }

    @Test
    public void testShouldRetry() throws Exception {
        TestTask task = new TestTask(Duration.ofMillis(100), 1L, 1, true);
        CompletableFuture<Void> future = task.run(tenantInboxInstance, runner);
        future.get();
        Assert.assertTrue(task.callOperationInvoked);

        verify(runner, Mockito.times(1)).schedule(Mockito.eq(tenantInboxInstance), Mockito.any());
        ArgumentCaptor<RetryableDelayedTask<Boolean>> taskCaptor = ArgumentCaptor.forClass(
            RetryableDelayedTask.class);
        verify(runner).schedule(Mockito.eq(tenantInboxInstance), taskCaptor.capture());
        RetryableDelayedTask<Boolean> retryTask = taskCaptor.getValue();
        Assert.assertEquals(retryTask.retryCount, task.retryCount + 1);
    }

    @Test
    public void testNoRetry() throws Exception {
        TestTask task = new TestTask(Duration.ofMillis(100), 1L, 1, false);
        CompletableFuture<Void> future = task.run(tenantInboxInstance, runner);
        future.get();  // 等待任务执行完成
        Assert.assertTrue(task.callOperationInvoked);

        verify(runner, Mockito.never()).schedule(Mockito.any(), Mockito.any());
    }

    private static class TestTask extends RetryableDelayedTask<Boolean> {
        private final Boolean response;
        boolean callOperationInvoked = false;

        public TestTask(Duration delay, long version, int retryCount, Boolean response) {
            super(delay, version, retryCount);
            this.response = response;
        }

        @Override
        protected CompletableFuture<Boolean> callOperation(TenantInboxInstance key,
                                                           IDelayTaskRunner<TenantInboxInstance> runner) {
            callOperationInvoked = true;
            return CompletableFuture.completedFuture(response);
        }

        @Override
        protected boolean shouldRetry(Boolean reply) {
            return reply;
        }

        @Override
        protected RetryableDelayedTask<Boolean> createRetryTask(Duration newDelay) {
            return new TestTask(newDelay, mod, retryCount + 1, response);
        }

        @Override
        protected String getTaskName() {
            return "TestTask";
        }
    }
}