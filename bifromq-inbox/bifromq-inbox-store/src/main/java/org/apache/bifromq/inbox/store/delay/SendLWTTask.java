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

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.inbox.record.TenantInboxInstance;
import org.apache.bifromq.inbox.rpc.proto.SendLWTReply;
import org.apache.bifromq.inbox.rpc.proto.SendLWTRequest;
import org.apache.bifromq.inbox.storage.proto.InboxVersion;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Retry-able task for sending last will message after a delay.
 */
public class SendLWTTask extends RetryableDelayedTask<SendLWTReply> {
    private final IInboxClient inboxClient;

    public SendLWTTask(Duration delay, long version, IInboxClient inboxClient) {
        this(delay, version, inboxClient, 0);
    }

    private SendLWTTask(Duration delay, long version, IInboxClient inboxClient, int retryCount) {
        super(delay, version, retryCount);
        this.inboxClient = inboxClient;
    }

    @Override
    protected CompletableFuture<SendLWTReply> callOperation(TenantInboxInstance key,
                                                            IDelayTaskRunner<TenantInboxInstance> runner) {
        return inboxClient.sendLWT(SendLWTRequest.newBuilder()
            .setReqId(System.nanoTime())
            .setTenantId(key.tenantId())
            .setInboxId(key.instance().inboxId())
            .setVersion(InboxVersion.newBuilder()
                .setIncarnation(key.instance().incarnation())
                .setMod(mod)
                .build())
            .setNow(HLC.INST.getPhysical())
            .build());
    }

    @Override
    protected boolean shouldRetry(SendLWTReply reply) {
        return reply.getCode() == SendLWTReply.Code.TRY_LATER
            || reply.getCode() == SendLWTReply.Code.BACK_PRESSURE_REJECTED;
    }

    @Override
    protected RetryableDelayedTask<SendLWTReply> createRetryTask(Duration newDelay) {
        return new SendLWTTask(newDelay, mod, inboxClient);
    }

    @Override
    protected String getTaskName() {
        return "Send LWT";
    }
}
