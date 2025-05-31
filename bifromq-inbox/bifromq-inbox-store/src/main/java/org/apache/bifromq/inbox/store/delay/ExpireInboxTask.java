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

import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.record.TenantInboxInstance;
import org.apache.bifromq.inbox.rpc.proto.DeleteReply;
import org.apache.bifromq.inbox.rpc.proto.DeleteRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Retry-able task for expiring an inbox after a delay.
 */
public class ExpireInboxTask extends RetryableDelayedTask<DeleteReply> {
    private final IInboxClient inboxClient;

    public ExpireInboxTask(Duration delay, long mod, IInboxClient inboxClient) {
        this(delay, mod, inboxClient, 0);
    }

    private ExpireInboxTask(Duration delay, long version, IInboxClient inboxClient, int retryCount) {
        super(delay, version, retryCount);
        this.inboxClient = inboxClient;
    }

    @Override
    protected CompletableFuture<DeleteReply> callOperation(TenantInboxInstance key,
                                                           IDelayTaskRunner<TenantInboxInstance> runner) {
        return inboxClient.delete(DeleteRequest.newBuilder()
            .setReqId(System.nanoTime())
            .setTenantId(key.tenantId())
            .setInboxId(key.instance().inboxId())
            .setVersion(InboxVersion.newBuilder()
                .setIncarnation(key.instance().incarnation())
                .setMod(mod)
                .build())
            .build());
    }

    @Override
    protected boolean shouldRetry(DeleteReply reply) {
        return reply.getCode() == DeleteReply.Code.TRY_LATER
            || reply.getCode() == DeleteReply.Code.BACK_PRESSURE_REJECTED;
    }

    @Override
    protected RetryableDelayedTask<DeleteReply> createRetryTask(Duration newDelay) {
        return new ExpireInboxTask(newDelay, mod, inboxClient, retryCount + 1);
    }

    @Override
    protected String getTaskName() {
        return "Expire inbox";
    }
}
