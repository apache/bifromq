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

import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxInstanceStartKey;

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.inbox.storage.proto.Fetched;
import org.apache.bifromq.sysprops.props.InboxFetchQueuesPerRange;

@Slf4j
public class InboxFetchScheduler extends InboxReadScheduler<FetchRequest, Fetched, BatchFetchCall>
    implements IInboxFetchScheduler {
    public InboxFetchScheduler(IBaseKVStoreClient inboxStoreClient) {
        super(BatchFetchCall::new, InboxFetchQueuesPerRange.INSTANCE.get(), inboxStoreClient);
    }

    @Override
    protected int selectQueue(FetchRequest request) {
        // use Murmur3_32 to improve distribution and reduce low-bit modulo bias
        int hash = Hashing.murmur3_32_fixed()
            .newHasher()
            .putString(request.tenantId(), StandardCharsets.UTF_8)
            .putString(request.inboxId(), StandardCharsets.UTF_8)
            .putLong(request.incarnation())
            .hash()
            .asInt();
        return Math.floorMod(hash, queuesPerRange);
    }

    @Override
    protected boolean isLinearizable(FetchRequest request) {
        return false;
    }

    @Override
    protected ByteString rangeKey(FetchRequest request) {
        return inboxInstanceStartKey(request.tenantId(), request.inboxId(), request.incarnation());
    }
}
