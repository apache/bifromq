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

package org.apache.bifromq.mqtt.inbox;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;

import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.baserpc.client.exception.ServerNotFoundException;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryPackage;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryRequest;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.plugin.subbroker.DeliveryResults;
import org.apache.bifromq.plugin.subbroker.IDeliverer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.mqtt.inbox.rpc.proto.WriteReply;
import org.apache.bifromq.mqtt.inbox.rpc.proto.WriteRequest;
import org.apache.bifromq.type.MatchInfo;

@Slf4j
class DeliveryPipeline implements IDeliverer {
    private final IRPCClient.IRequestPipeline<WriteRequest, WriteReply> ppln;

    DeliveryPipeline(IRPCClient.IRequestPipeline<WriteRequest, WriteReply> ppln) {
        this.ppln = ppln;
    }

    @Override
    public CompletableFuture<DeliveryReply> deliver(DeliveryRequest request) {
        long reqId = System.nanoTime();
        return ppln.invoke(WriteRequest.newBuilder()
                .setReqId(reqId)
                .setRequest(request)
                .build())
            .thenApply(WriteReply::getReply)
            .exceptionally(unwrap(e -> {
                if (e instanceof ServerNotFoundException) {
                    DeliveryReply.Builder replyBuilder = DeliveryReply.newBuilder().setCode(DeliveryReply.Code.OK);
                    Set<MatchInfo> allMatchInfos = new HashSet<>();
                    for (String tenantId : request.getPackageMap().keySet()) {
                        DeliveryResults.Builder resultsBuilder = DeliveryResults.newBuilder();
                        DeliveryPackage deliveryPackage = request.getPackageMap().get(tenantId);
                        for (DeliveryPack pack : deliveryPackage.getPackList()) {
                            allMatchInfos.addAll(pack.getMatchInfoList());
                        }
                        for (MatchInfo matchInfo : allMatchInfos) {
                            resultsBuilder.addResult(DeliveryResult.newBuilder().setMatchInfo(matchInfo)
                                .setCode(DeliveryResult.Code.NO_RECEIVER).build());
                        }
                        replyBuilder.putResult(tenantId, resultsBuilder.build());
                    }
                    return replyBuilder.build();
                }
                log.debug("Failed to deliver request: {}", request, e);
                return DeliveryReply.newBuilder().setCode(DeliveryReply.Code.ERROR).build();
            }));
    }

    @Override
    public void close() {
        ppln.close();
    }
}
