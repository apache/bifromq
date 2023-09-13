/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.dist.client;

import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.baserpc.IRPCClient;
import com.baidu.bifromq.dist.RPCBluePrint;
import com.baidu.bifromq.dist.client.scheduler.DistServerCall;
import com.baidu.bifromq.dist.client.scheduler.DistServerCallScheduler2;
import com.baidu.bifromq.dist.client.scheduler.IDistServerCallScheduler;
import com.baidu.bifromq.dist.rpc.proto.ClearRequest;
import com.baidu.bifromq.dist.rpc.proto.DistServiceGrpc;
import com.baidu.bifromq.dist.rpc.proto.SubReply;
import com.baidu.bifromq.dist.rpc.proto.SubRequest;
import com.baidu.bifromq.dist.rpc.proto.UnsubRequest;
import com.baidu.bifromq.type.ClientInfo;
import com.baidu.bifromq.type.Message;
import com.baidu.bifromq.type.QoS;
import io.reactivex.rxjava3.core.Observable;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class DistClient implements IDistClient {
    private final IDistServerCallScheduler reqScheduler;
    private final IRPCClient rpcClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DistClient(DistClientBuilder builder) {
        this.rpcClient = IRPCClient.newBuilder()
            .bluePrint(RPCBluePrint.INSTANCE)
            .executor(builder.executor)
            .eventLoopGroup(builder.eventLoopGroup)
            .crdtService(builder.crdtService)
            .sslContext(builder.sslContext)
            .build();
//        reqScheduler = new DistServerCallScheduler(rpcClient);
        reqScheduler = new DistServerCallScheduler2(rpcClient);
    }

    @Override
    public Observable<IRPCClient.ConnState> connState() {
        return rpcClient.connState();
    }

    @Override
    public CompletableFuture<Void> pub(long reqId, String topic, QoS qos, ByteBuffer payload,
                                       int expirySeconds, ClientInfo publisher) {
        long now = HLC.INST.getPhysical();
        long expiry = expirySeconds == Integer.MAX_VALUE ? Long.MAX_VALUE :
            now + TimeUnit.MILLISECONDS.convert(expirySeconds, TimeUnit.SECONDS);
        return reqScheduler.schedule(new DistServerCall(publisher, topic, Message.newBuilder()
            .setMessageId(reqId)
            .setPubQoS(qos)
            .setPayload(unsafeWrap(payload))
            .setTimestamp(now)
            .setExpireTimestamp(expiry)
            .build()));
    }

    @Override
    public CompletableFuture<Integer> sub(long reqId, String tenantId, String topicFilter, QoS qos, String inboxId,
                                          String delivererKey, int subBrokerId) {
        SubRequest request = SubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setTopicFilter(topicFilter)
            .setSubQoS(qos)
            .setInboxId(inboxId)
            .setDelivererKey(delivererKey)
            .setBroker(subBrokerId)
            .build();
        log.trace("Handling sub request:\n{}", request);
        return rpcClient.invoke(tenantId, null, request, DistServiceGrpc.getSubMethod())
            .handle((v, e) -> {
                if (e != null) {
                    log.debug("Sub request failed: reqId={}, tenantId={}", request.getReqId(), tenantId, e);
                    return SubReply.SubResult.Failure.getNumber();
                }
                log.trace("Finish handling sub request:\n{}, reply:\n{}", request, v);
                return v.getResult().getNumber();
            });

    }

    @Override
    public CompletableFuture<Boolean> unsub(long reqId, String tenantId, String topicFilter, String inbox,
                                            String delivererKey, int subBrokerId) {
        UnsubRequest request = UnsubRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setTopicFilter(topicFilter)
            .setInboxId(inbox)
            .setDelivererKey(delivererKey)
            .setBroker(subBrokerId)
            .build();
        log.trace("Handling unsub request:\n{}", request);
        return rpcClient.invoke(tenantId, null, request, DistServiceGrpc.getUnsubMethod())
            .thenApply(v -> {
                log.trace("Finish handling unsub request:\n{}", request);
                return v.getExist();
            });
    }

    @Override
    public CompletableFuture<Void> clear(long reqId, String tenantId, String inboxId, String delivererKey,
                                         int subBrokerId) {
        log.trace("Requesting clear: reqId={}", reqId);
        ClearRequest request = ClearRequest.newBuilder()
            .setReqId(reqId)
            .setTenantId(tenantId)
            .setInboxId(inboxId)
            .setDelivererKey(delivererKey)
            .setBroker(subBrokerId)
            .build();
        return rpcClient.invoke(tenantId, null, request, DistServiceGrpc.getClearMethod())
            .thenApply(v -> {
                log.trace("Got clear reply: request={}, reply={}", request, v);
                return null;
            });
    }

    @Override
    public void stop() {
        // close tenant logger and drain logs before closing the dist client
        if (closed.compareAndSet(false, true)) {
            log.info("Stopping dist client");
            log.debug("Closing request scheduler");
            reqScheduler.close();
            log.debug("Stopping rpc client");
            rpcClient.stop();
            log.info("Dist client stopped");
        }
    }
}
