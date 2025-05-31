/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.inbox.server;

import static org.apache.bifromq.base.util.CompletableFutureUtil.unwrap;
import static org.apache.bifromq.plugin.subbroker.TypeUtil.toResult;

import org.apache.bifromq.base.util.AsyncRetry;
import org.apache.bifromq.base.util.exception.RetryTimeoutException;
import org.apache.bifromq.basekv.client.exception.BadVersionException;
import org.apache.bifromq.basekv.client.exception.TryLaterException;
import org.apache.bifromq.basescheduler.exception.BackPressureException;
import org.apache.bifromq.basescheduler.exception.BatcherUnavailableException;
import org.apache.bifromq.inbox.record.TenantInboxInstance;
import org.apache.bifromq.inbox.rpc.proto.SendReply;
import org.apache.bifromq.inbox.rpc.proto.SendRequest;
import org.apache.bifromq.inbox.server.scheduler.IInboxInsertScheduler;
import org.apache.bifromq.inbox.storage.proto.InsertRequest;
import org.apache.bifromq.inbox.storage.proto.InsertResult;
import org.apache.bifromq.inbox.storage.proto.SubMessagePack;
import org.apache.bifromq.plugin.subbroker.DeliveryPack;
import org.apache.bifromq.plugin.subbroker.DeliveryReply;
import org.apache.bifromq.plugin.subbroker.DeliveryResult;
import org.apache.bifromq.sysprops.props.DataPlaneMaxBurstLatencyMillis;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.TopicUtil;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class InboxWriter implements InboxWriterPipeline.ISendRequestHandler {
    private final IInboxInsertScheduler insertScheduler;
    private final long retryTimeoutNanos;

    InboxWriter(IInboxInsertScheduler insertScheduler) {
        this.insertScheduler = insertScheduler;
        this.retryTimeoutNanos = Duration.ofMillis(DataPlaneMaxBurstLatencyMillis.INSTANCE.get()).toNanos();
    }

    @Override
    public CompletableFuture<SendReply> handle(SendRequest request) {
        Map<TenantInboxInstance, List<MatchInfo>> matchInfosByInbox = new HashMap<>();
        Map<TenantInboxInstance, List<SubMessagePack>> subMsgPacksByInbox = new HashMap<>();
        // break DeliveryPack into SubMessagePack by each TenantInboxInstance
        for (String tenantId : request.getRequest().getPackageMap().keySet()) {
            for (DeliveryPack pack : request.getRequest().getPackageMap().get(tenantId).getPackList()) {
                TopicMessagePack topicMessagePack = pack.getMessagePack();
                Map<TenantInboxInstance, SubMessagePack.Builder> subMsgPackByInbox = new HashMap<>();
                for (MatchInfo matchInfo : pack.getMatchInfoList()) {
                    TenantInboxInstance tenantInboxInstance = TenantInboxInstance.from(tenantId, matchInfo);
                    matchInfosByInbox.computeIfAbsent(tenantInboxInstance, k -> new LinkedList<>()).add(matchInfo);
                    subMsgPackByInbox.computeIfAbsent(tenantInboxInstance,
                            k -> SubMessagePack.newBuilder().setMessages(topicMessagePack))
                        .putMatchedTopicFilters(matchInfo.getMatcher().getMqttTopicFilter(),
                            matchInfo.getIncarnation());
                }
                for (TenantInboxInstance tenantInboxInstance : subMsgPackByInbox.keySet()) {
                    subMsgPacksByInbox.computeIfAbsent(tenantInboxInstance, k -> new LinkedList<>())
                        .add(subMsgPackByInbox.get(tenantInboxInstance).build());
                }
            }
        }
        List<CompletableFuture<InsertResult>> replyFutures = subMsgPacksByInbox
            .entrySet()
            .stream()
            .map(entry -> {
                InsertRequest insertRequest = InsertRequest.newBuilder()
                    .setTenantId(entry.getKey().tenantId())
                    .setInboxId(entry.getKey().instance().inboxId())
                    .setIncarnation(entry.getKey().instance().incarnation())
                    .addAllMessagePack(entry.getValue()).build();
                return AsyncRetry.exec(() -> insertScheduler.schedule(insertRequest), (v, e) -> {
                    if (e == null) {
                        return false;
                    }
                    return e instanceof BatcherUnavailableException
                        || e instanceof TryLaterException
                        || e instanceof BadVersionException;
                }, retryTimeoutNanos / 5, retryTimeoutNanos);
            }).toList();
        return CompletableFuture.allOf(replyFutures.toArray(new CompletableFuture[0]))
            .handle(unwrap((v, e) -> {
                if (e != null) {
                    if (e instanceof RetryTimeoutException) {
                        return SendReply.newBuilder().setReqId(request.getReqId())
                            .setReply(DeliveryReply.newBuilder()
                                .setCode(DeliveryReply.Code.BACK_PRESSURE_REJECTED)
                                .build())
                            .build();
                    }
                    if (e instanceof BackPressureException) {
                        return SendReply.newBuilder().setReqId(request.getReqId())
                            .setReply(DeliveryReply.newBuilder()
                                .setCode(DeliveryReply.Code.BACK_PRESSURE_REJECTED)
                                .build())
                            .build();
                    }
                    log.debug("Failed to insert", e);
                    return SendReply.newBuilder().setReqId(request.getReqId())
                        .setReply(DeliveryReply.newBuilder().setCode(DeliveryReply.Code.ERROR).build()).build();
                }
                assert replyFutures.size() == subMsgPacksByInbox.size();
                SendReply.Builder replyBuilder = SendReply.newBuilder().setReqId(request.getReqId());
                Map<String, Map<MatchInfo, DeliveryResult.Code>> tenantMatchResultMap = new HashMap<>();
                int i = 0;
                for (TenantInboxInstance tenantInboxInstance : subMsgPacksByInbox.keySet()) {
                    String receiverId = tenantInboxInstance.receiverId();
                    InsertResult result = replyFutures.get(i++).join();
                    Map<MatchInfo, DeliveryResult.Code> matchResultMap =
                        tenantMatchResultMap.computeIfAbsent(tenantInboxInstance.tenantId(), k -> new HashMap<>());
                    switch (result.getCode()) {
                        case OK -> result.getResultList().forEach(insertionResult -> {
                            DeliveryResult.Code code =
                                insertionResult.getRejected() ? DeliveryResult.Code.NO_SUB : DeliveryResult.Code.OK;
                            matchResultMap.putIfAbsent(MatchInfo.newBuilder().setReceiverId(receiverId)
                                .setMatcher(TopicUtil.from(insertionResult.getTopicFilter()))
                                .setIncarnation(insertionResult.getIncarnation()).build(), code);
                        });
                        case NO_INBOX -> {
                            for (MatchInfo matchInfo : matchInfosByInbox.get(tenantInboxInstance)) {
                                matchResultMap.putIfAbsent(matchInfo, DeliveryResult.Code.NO_RECEIVER);
                            }
                        }
                        default -> {
                            // never happen
                        }
                    }
                }
                return replyBuilder.setReqId(request.getReqId()).setReply(
                    DeliveryReply.newBuilder().setCode(DeliveryReply.Code.OK)
                        .putAllResult(toResult(tenantMatchResultMap))
                        .build()).build();
            }));
    }
}
