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

package org.apache.bifromq.inbox.server.scheduler;

import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.scheduler.MutationCallScheduler;
import org.apache.bifromq.inbox.rpc.proto.SubReply;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.sysprops.props.ControlPlaneMaxBurstLatencyMillis;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InboxSubScheduler extends MutationCallScheduler<SubRequest, SubReply, BatchSubCall>
    implements IInboxSubScheduler {
    public InboxSubScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(BatchSubCall::new, Duration.ofMillis(ControlPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos(),
            inboxStoreClient);
    }

    @Override
    protected ByteString rangeKey(SubRequest request) {
        return inboxStartKeyPrefix(request.getTenantId(), request.getInboxId());
    }
}
