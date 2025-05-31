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

package org.apache.bifromq.sessiondict.server;

import static org.apache.bifromq.baserpc.server.UnaryResponse.response;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CHANNEL_ID_KEY;
import static org.apache.bifromq.type.MQTTClientInfoConstants.MQTT_CLIENT_BROKER_KEY;

import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.mqtt.inbox.IMqttBrokerClient;
import org.apache.bifromq.sessiondict.rpc.proto.ExistReply;
import org.apache.bifromq.sessiondict.rpc.proto.ExistRequest;
import org.apache.bifromq.sessiondict.rpc.proto.GetReply;
import org.apache.bifromq.sessiondict.rpc.proto.GetRequest;
import org.apache.bifromq.sessiondict.rpc.proto.KillAllReply;
import org.apache.bifromq.sessiondict.rpc.proto.KillAllRequest;
import org.apache.bifromq.sessiondict.rpc.proto.KillReply;
import org.apache.bifromq.sessiondict.rpc.proto.KillRequest;
import org.apache.bifromq.sessiondict.rpc.proto.Quit;
import org.apache.bifromq.sessiondict.rpc.proto.Session;
import org.apache.bifromq.sessiondict.rpc.proto.SessionDictServiceGrpc;
import org.apache.bifromq.sessiondict.rpc.proto.SubReply;
import org.apache.bifromq.sessiondict.rpc.proto.SubRequest;
import org.apache.bifromq.sessiondict.rpc.proto.UnsubReply;
import org.apache.bifromq.sessiondict.rpc.proto.UnsubRequest;
import org.apache.bifromq.type.ClientInfo;

@Slf4j
class SessionDictService extends SessionDictServiceGrpc.SessionDictServiceImplBase {
    private final ISessionRegistry sessionRegistry;
    private final IMqttBrokerClient mqttBrokerClient;

    SessionDictService(IMqttBrokerClient mqttBrokerClient) {
        this.mqttBrokerClient = mqttBrokerClient;
        this.sessionRegistry = new SessionRegistry();
    }

    @Override
    public StreamObserver<Session> dict(StreamObserver<Quit> responseObserver) {
        return new SessionRegister(((sessionOwner, reg, register) -> {
            if (reg) {
                sessionRegistry.add(sessionOwner, register);
            } else {
                sessionRegistry.remove(sessionOwner, register);
            }
        }), responseObserver);
    }

    @Override
    public void kill(KillRequest request, StreamObserver<KillReply> responseObserver) {
        response(ignore -> {
            String tenantId = request.getTenantId();
            Optional<ISessionRegistry.SessionRegistration> reg =
                sessionRegistry.findRegistration(tenantId, request.getUserId(), request.getClientId());
            reg.ifPresent(
                sessionRegistration -> sessionRegistration.stop(request.getKiller(), request.getServerRedirection()));
            return CompletableFuture.completedFuture(KillReply.newBuilder()
                .setReqId(request.getReqId())
                .setResult(KillReply.Result.OK)
                .build());
        }, responseObserver);
    }

    @Override
    public void killAll(KillAllRequest request, StreamObserver<KillAllReply> responseObserver) {
        response(ignore -> {
            String tenantId = request.getTenantId();
            Iterable<ISessionRegistry.SessionRegistration> sessionRegistrations;
            if (request.hasUserId()) {
                sessionRegistrations = sessionRegistry.findRegistrations(tenantId, request.getUserId());
            } else {
                sessionRegistrations = sessionRegistry.findRegistrations(tenantId);
            }
            try {
                // TODO: support disconnect a constant rate
                sessionRegistrations.forEach(reg -> reg.stop(request.getKiller(), request.getServerRedirection()));
                return CompletableFuture.completedFuture(KillAllReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(KillAllReply.Result.OK)
                    .build());
            } catch (Throwable e) {
                log.debug("Failed to kick", e);
                return CompletableFuture.completedFuture(KillAllReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(KillAllReply.Result.ERROR)
                    .build());
            }
        }, responseObserver);
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetReply> responseObserver) {
        response(tenantId -> {
            Optional<ClientInfo> sessionOwner =
                sessionRegistry.get(tenantId, request.getUserId(), request.getClientId());
            return sessionOwner.map(clientInfo -> CompletableFuture.completedFuture(GetReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(GetReply.Result.OK)
                    .setOwner(clientInfo)
                    .build()))
                .orElseGet(() -> CompletableFuture.completedFuture(GetReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(GetReply.Result.NOT_FOUND)
                    .build()));
        }, responseObserver);
    }

    @Override
    public StreamObserver<ExistRequest> exist(StreamObserver<ExistReply> responseObserver) {
        return new SessionExistPipeline(sessionRegistry, responseObserver);
    }

    @Override
    public void sub(SubRequest request, StreamObserver<SubReply> responseObserver) {
        response(tenantId -> {
            Optional<ClientInfo> sessionOwner =
                sessionRegistry.get(tenantId, request.getUserId(), request.getClientId());
            if (sessionOwner.isEmpty()) {
                return CompletableFuture.completedFuture(SubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(SubReply.Result.NO_SESSION)
                    .build());
            } else {
                return mqttBrokerClient.sub(request.getReqId(),
                        request.getTenantId(),
                        sessionOwner.get().getMetadataOrDefault(MQTT_CHANNEL_ID_KEY, ""),
                        request.getTopicFilter(),
                        request.getQos(),
                        sessionOwner.get().getMetadataOrDefault(MQTT_CLIENT_BROKER_KEY, ""))
                    .thenApply(r -> SubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(SubReply.Result.forNumber(r.getResult().getNumber()))
                        .build());
            }
        }, responseObserver);
    }

    @Override
    public void unsub(UnsubRequest request, StreamObserver<UnsubReply> responseObserver) {
        response(tenantId -> {
            Optional<ClientInfo> sessionOwner =
                sessionRegistry.get(tenantId, request.getUserId(), request.getClientId());

            if (sessionOwner.isEmpty()) {
                return CompletableFuture.completedFuture(UnsubReply.newBuilder()
                    .setReqId(request.getReqId())
                    .setResult(UnsubReply.Result.NO_SESSION)
                    .build());
            } else {
                return mqttBrokerClient.unsub(request.getReqId(),
                        request.getTenantId(),
                        sessionOwner.get().getMetadataOrDefault(MQTT_CHANNEL_ID_KEY, ""),
                        request.getTopicFilter(),
                        sessionOwner.get().getMetadataOrDefault(MQTT_CLIENT_BROKER_KEY, ""))
                    .thenApply(r -> UnsubReply.newBuilder()
                        .setReqId(request.getReqId())
                        .setResult(UnsubReply.Result.forNumber(r.getResult().getNumber()))
                        .build());
            }
        }, responseObserver);
    }

    void close() {
        sessionRegistry.close();
    }
}
