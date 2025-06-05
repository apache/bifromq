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

package org.apache.bifromq.sessiondict.client.scheduler;

import static org.apache.bifromq.sessiondict.SessionRegisterKeyUtil.parseTenantId;

import org.apache.bifromq.baserpc.client.IRPCClient;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import org.apache.bifromq.basescheduler.BatchCallScheduler;
import org.apache.bifromq.basescheduler.IBatchCall;
import org.apache.bifromq.basescheduler.IBatchCallBuilder;
import org.apache.bifromq.sessiondict.SessionRegisterKeyUtil;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckRequest;
import org.apache.bifromq.sessiondict.client.type.OnlineCheckResult;
import org.apache.bifromq.sessiondict.rpc.proto.ExistReply;
import org.apache.bifromq.sessiondict.rpc.proto.ExistRequest;
import org.apache.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import org.apache.bifromq.sysprops.props.DataPlaneMaxBurstLatencyMillis;

/**
 * Scheduler for checking the online status of a session.
 */
public class OnlineCheckScheduler extends BatchCallScheduler<OnlineCheckRequest, OnlineCheckResult, String>
    implements IOnlineCheckScheduler {

    public OnlineCheckScheduler(IRPCClient rpcClient) {
        super((name, batcherKey) -> new IBatchCallBuilder<>() {
                private final IRPCClient.IRequestPipeline<ExistRequest, ExistReply> ppln
                    = rpcClient.createRequestPipeline(parseTenantId(batcherKey), null, batcherKey,
                    Collections.emptyMap(), SessionDictServiceGrpc.getExistMethod());

                @Override
                public IBatchCall<OnlineCheckRequest, OnlineCheckResult, String> newBatchCall() {
                    return new BatchSessionExistCall(ppln);
                }

                @Override
                public void close() {
                    ppln.close();
                }
            },
            Duration.ofMillis(DataPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos());
    }

    @Override
    protected Optional<String> find(OnlineCheckRequest call) {
        return Optional.of(SessionRegisterKeyUtil.toRegisterKey(call.tenantId(), call.userId(), call.clientId()));
    }
}
