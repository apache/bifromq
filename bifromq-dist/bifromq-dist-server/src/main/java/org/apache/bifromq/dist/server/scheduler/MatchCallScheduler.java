/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.dist.server.scheduler;

import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toGroupRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toNormalRouteKey;
import static org.apache.bifromq.dist.worker.schema.KVSchemaUtil.toReceiverUrl;

import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.basekv.client.scheduler.MutationCallScheduler;
import org.apache.bifromq.dist.rpc.proto.MatchReply;
import org.apache.bifromq.dist.rpc.proto.MatchRequest;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sysprops.props.ControlPlaneMaxBurstLatencyMillis;
import org.apache.bifromq.type.RouteMatcher;
import com.google.protobuf.ByteString;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MatchCallScheduler extends MutationCallScheduler<MatchRequest, MatchReply, BatchMatchCall>
    implements IMatchCallScheduler {

    public MatchCallScheduler(IBaseKVStoreClient distWorkerClient, ISettingProvider settingProvider) {
        super((pipeline, batcherKey) -> new BatchMatchCall(pipeline, settingProvider, batcherKey),
            Duration.ofMillis(ControlPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos(),
            distWorkerClient);
    }

    protected ByteString rangeKey(MatchRequest call) {
        if (call.getMatcher().getType() == RouteMatcher.Type.Normal) {
            return toNormalRouteKey(call.getTenantId(), call.getMatcher(),
                toReceiverUrl(call.getBrokerId(), call.getReceiverId(), call.getDelivererKey()));
        } else {
            return toGroupRouteKey(call.getTenantId(), call.getMatcher());
        }
    }
}
