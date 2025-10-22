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

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.inbox.rpc.proto.InboxStateReply;
import org.apache.bifromq.inbox.rpc.proto.InboxStateRequest;
import org.apache.bifromq.sysprops.props.InboxCheckQueuesPerRange;

@Slf4j
public class InboxFetchStateScheduler
    extends InboxReadScheduler<InboxStateRequest, InboxStateReply, BatchInboxStateCall>
    implements IInboxFetchStateScheduler {

    public InboxFetchStateScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(BatchInboxStateCall::new, InboxCheckQueuesPerRange.INSTANCE.get(), inboxStoreClient);
    }

    @Override
    protected boolean isLinearizable(InboxStateRequest request) {
        return false;
    }

    @Override
    protected ByteString rangeKey(InboxStateRequest request) {
        return inboxStartKeyPrefix(request.getTenantId(), request.getInboxId());
    }
}
