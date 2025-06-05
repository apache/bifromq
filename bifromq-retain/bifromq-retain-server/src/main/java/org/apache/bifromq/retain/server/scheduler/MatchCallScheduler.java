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

package org.apache.bifromq.retain.server.scheduler;

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basescheduler.BatchCallScheduler;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sysprops.props.DataPlaneMaxBurstLatencyMillis;
import java.time.Duration;
import java.util.Optional;

public class MatchCallScheduler
    extends BatchCallScheduler<MatchRetainedRequest, MatchRetainedResult, MatchCallBatcherKey>
    implements IMatchCallScheduler {

    public MatchCallScheduler(IBaseKVStoreClient retainStoreClient, ISettingProvider settingProvider) {
        super((name, batcherKey) -> () -> new BatchMatchCall(batcherKey, retainStoreClient, settingProvider),
            Duration.ofSeconds(DataPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos());
    }

    @Override
    protected Optional<MatchCallBatcherKey> find(MatchRetainedRequest call) {
        // TODO: implement multi batcher for tenant
        return Optional.of(new MatchCallBatcherKey(call.tenantId(), 0));
    }
}
