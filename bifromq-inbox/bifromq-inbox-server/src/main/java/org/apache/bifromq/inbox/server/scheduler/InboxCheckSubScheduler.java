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

package org.apache.bifromq.inbox.server.scheduler;

import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.plugin.subbroker.CheckReply;
import org.apache.bifromq.sysprops.props.InboxCheckQueuesPerRange;
import com.google.protobuf.ByteString;

public class InboxCheckSubScheduler extends InboxReadScheduler<CheckMatchInfo, CheckReply.Code, BatchCheckSubCall>
    implements IInboxCheckSubScheduler {
    public InboxCheckSubScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(BatchCheckSubCall::new, InboxCheckQueuesPerRange.INSTANCE.get(), inboxStoreClient);
    }

    @Override
    protected boolean isLinearizable(CheckMatchInfo request) {
        return true;
    }

    @Override
    protected ByteString rangeKey(CheckMatchInfo request) {
        return inboxStartKeyPrefix(request.tenantId(), request.matchInfo().getReceiverId());
    }
}
