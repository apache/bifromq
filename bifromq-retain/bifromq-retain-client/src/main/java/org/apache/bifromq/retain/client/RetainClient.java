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

package org.apache.bifromq.retain.client;

import static org.apache.bifromq.metrics.TenantMetric.MqttIngressRetainBytes;
import static org.apache.bifromq.metrics.TenantMetric.MqttRetainMatchCount;
import static org.apache.bifromq.metrics.TenantMetric.MqttRetainedBytes;

import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.baserpc.client.IRPCClient;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.retain.rpc.proto.ExpireAllReply;
import org.apache.bifromq.retain.rpc.proto.ExpireAllRequest;
import org.apache.bifromq.retain.rpc.proto.MatchReply;
import org.apache.bifromq.retain.rpc.proto.MatchRequest;
import org.apache.bifromq.retain.rpc.proto.RetainReply;
import org.apache.bifromq.retain.rpc.proto.RetainRequest;
import org.apache.bifromq.retain.rpc.proto.RetainServiceGrpc;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import org.apache.bifromq.type.QoS;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class RetainClient implements IRetainClient {
    private final IRPCClient rpcClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    RetainClient(IRPCClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.debug("Stopping retain client");
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.debug("Retain client stopped");
        }
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public CompletableFuture<MatchReply> match(MatchRequest request) {
        ITenantMeter tenantMeter = ITenantMeter.get(request.getTenantId());
        tenantMeter.recordCount(MqttRetainMatchCount);
        return rpcClient.invoke(request.getTenantId(), null, request, RetainServiceGrpc.getMatchMethod())
            .exceptionally(e -> {
                log.debug("Failed to match", e);
                return MatchReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(MatchReply.Result.ERROR)
                    .build();
            });
    }

    @Override
    public CompletableFuture<RetainReply> retain(long reqId, String topic, QoS qos, ByteString payload,
                                                 int expirySeconds, ClientInfo publisher) {
        ITenantMeter tenantMeter = ITenantMeter.get(publisher.getTenantId());
        if (!payload.isEmpty()) {
            tenantMeter.recordSummary(MqttIngressRetainBytes, payload.size());
        }
        // TODO: add throttler
        return rpcClient.invoke(publisher.getTenantId(), null, RetainRequest.newBuilder()
                .setReqId(reqId)
                .setTopic(topic)
                .setMessage(Message.newBuilder()
                    .setMessageId(reqId)
                    .setPubQoS(qos)
                    .setPayload(payload)
                    .setTimestamp(HLC.INST.get())
                    .setExpiryInterval(expirySeconds)
                    .build())
                .setPublisher(publisher)
                .build(), RetainServiceGrpc.getRetainMethod())
            .exceptionally(e -> {
                log.debug("Failed to retain", e);
                return RetainReply.newBuilder()
                    .setReqId(reqId)
                    .setResult(RetainReply.Result.ERROR)
                    .build();
            })
            .thenApply(retainReply -> {
                if (retainReply.getResult() == RetainReply.Result.RETAINED) {
                    tenantMeter.recordSummary(MqttRetainedBytes, payload.size());
                }
                return retainReply;
            });
    }

    @Override
    public CompletableFuture<ExpireAllReply> expireAll(ExpireAllRequest request) {
        return rpcClient.invoke(request.getTenantId(), null, request, RetainServiceGrpc.getExpireAllMethod())
            .exceptionally(e -> {
                log.debug("Failed to retain", e);
                return ExpireAllReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(ExpireAllReply.Result.ERROR)
                    .build();
            });
    }
}
