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

package com.baidu.bifromq.apiserver.http.handler;

import static com.baidu.bifromq.apiserver.Headers.HEADER_DELIVERER_KEY;
import static com.baidu.bifromq.apiserver.Headers.HEADER_INBOX_ID;
import static com.baidu.bifromq.apiserver.Headers.HEADER_TOPIC_FILTER;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getDelivererKey;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getHeader;
import static com.baidu.bifromq.apiserver.http.handler.HTTPHeaderUtils.getRequiredSubBrokerId;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.baidu.bifromq.apiserver.http.IHTTPRequestHandler;
import com.baidu.bifromq.apiserver.utils.TopicUtil;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.dist.client.IDistClient;
import com.baidu.bifromq.dist.client.UnmatchResult;
import com.baidu.bifromq.inbox.client.IInboxClient;
import com.baidu.bifromq.inbox.rpc.proto.GetReply;
import com.baidu.bifromq.inbox.rpc.proto.GetRequest;
import com.baidu.bifromq.inbox.rpc.proto.UnsubReply;
import com.baidu.bifromq.inbox.rpc.proto.UnsubRequest;
import com.baidu.bifromq.inbox.storage.proto.InboxVersion;
import com.baidu.bifromq.mqtt.inbox.IMqttBrokerClient;
import com.baidu.bifromq.mqtt.inbox.MqttUnsubResult;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/unsub")
public final class HTTPUnsubHandler implements IHTTPRequestHandler {
    private final IMqttBrokerClient mqttBrokerClient;
    private final IInboxClient inboxClient;
    private final IDistClient distClient;
    private final ISettingProvider settingProvider;

    public HTTPUnsubHandler(IMqttBrokerClient mqttBrokerClient,
                            IInboxClient inboxClient,
                            IDistClient distClient,
                            ISettingProvider settingProvider) {
        this.mqttBrokerClient = mqttBrokerClient;
        this.inboxClient = inboxClient;
        this.distClient = distClient;
        this.settingProvider = settingProvider;
    }

    @DELETE
    @Operation(summary = "Remove a topic subscription from an inbox")
    @Parameters({
        @Parameter(name = "req_id", in = ParameterIn.HEADER, description = "optional caller provided request id", schema = @Schema(implementation = Long.class)),
        @Parameter(name = "tenant_id", in = ParameterIn.HEADER, required = true, description = "the tenant id"),
        @Parameter(name = "topic_filter", in = ParameterIn.HEADER, required = true, description = "the topic filter to remove"),
        @Parameter(name = "inbox_id", in = ParameterIn.HEADER, required = true, description = "the inbox for receiving subscribed messages"),
        @Parameter(name = "deliverer_key", in = ParameterIn.HEADER, description = "deliverer key for subBroker"),
        @Parameter(name = "subbroker_id", in = ParameterIn.HEADER, required = true, schema = @Schema(implementation = Integer.class), description = "the id of the subbroker hosting the inbox"),
    })
    @RequestBody(required = false)
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Success"),
        @ApiResponse(responseCode = "404", description = "Topic filter not found"),
    })

    @Override
    public CompletableFuture<FullHttpResponse> handle(@Parameter(hidden = true) long reqId,
                                                      @Parameter(hidden = true) String tenantId,
                                                      @Parameter(hidden = true) FullHttpRequest req) {
        try {
            String topicFilter = getHeader(HEADER_TOPIC_FILTER, req, true);
            String inboxId = getHeader(HEADER_INBOX_ID, req, true);
            int subBrokerId = getRequiredSubBrokerId(req);
            String delivererKey = getDelivererKey(HEADER_DELIVERER_KEY, req, subBrokerId);
            log.trace(
                "Handling http unsub request: reqId={}, tenantId={}, topicFilter={}, inboxId={}, subBrokerId={}",
                reqId, tenantId, topicFilter, inboxId, subBrokerId);
            if (!TopicUtil.checkTopicFilter(topicFilter, tenantId, settingProvider)) {
                return CompletableFuture.completedFuture(new DefaultFullHttpResponse(req.protocolVersion(),
                    FORBIDDEN, Unpooled.EMPTY_BUFFER));
            }
            CompletableFuture<FullHttpResponse> future;
            switch (subBrokerId) {
                case 0:
                    future = mqttBrokerClient.unsub(reqId, tenantId, inboxId, topicFilter)
                        .thenApply(v -> new DefaultFullHttpResponse(req.protocolVersion(),
                            v == MqttUnsubResult.OK ? OK : NOT_FOUND, Unpooled.EMPTY_BUFFER)
                        );
                    break;
                case 1:
                    future = inboxClient.get(GetRequest.newBuilder()
                            .setReqId(reqId)
                            .setTenantId(tenantId)
                            .setInboxId(inboxId)
                            .setNow(HLC.INST.getPhysical())
                            .build())
                        .thenCompose(getReply -> {
                            if (getReply.getCode() == GetReply.Code.EXIST) {
                                InboxVersion latest = getReply.getInbox(getReply.getInboxCount() - 1);
                                return inboxClient.unsub(UnsubRequest.newBuilder()
                                        .setReqId(reqId)
                                        .setTenantId(tenantId)
                                        .setInboxId(inboxId)
                                        .setIncarnation(latest.getIncarnation())
                                        .setVersion(latest.getVersion())
                                        .setTopicFilter(topicFilter)
                                        .setNow(HLC.INST.getPhysical())
                                        .build())
                                    .thenApply(unsubReply -> new DefaultFullHttpResponse(req.protocolVersion(),
                                        unsubReply.getCode() == UnsubReply.Code.OK ? OK : NOT_FOUND,
                                        Unpooled.EMPTY_BUFFER));
                            }
                            DefaultFullHttpResponse resp = new DefaultFullHttpResponse(req.protocolVersion(), NOT_FOUND,
                                Unpooled.EMPTY_BUFFER);
                            return CompletableFuture.completedFuture(resp);
                        });
                    break;
                default:
                    future = distClient.unmatch(reqId, tenantId, topicFilter, inboxId, delivererKey, subBrokerId)
                        .thenApply(v -> new DefaultFullHttpResponse(req.protocolVersion(),
                            v == UnmatchResult.OK ? OK : NOT_FOUND, Unpooled.EMPTY_BUFFER)
                        );
            }
            return future;
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
