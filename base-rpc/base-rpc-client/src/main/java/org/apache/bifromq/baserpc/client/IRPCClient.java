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

package org.apache.bifromq.baserpc.client;

import static java.util.Collections.emptyMap;

import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Observable;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The RPC client interface.
 */
public interface IRPCClient extends IConnectable {

    static RPCClientBuilder newBuilder() {
        return new RPCClientBuilder();
    }

    /**
     * The observable of live servers.
     *
     * @return an observable of connectable servers with a map of metadata attached
     */
    Observable<Map<String, Map<String, String>>> serverList();

    default <ReqT, RespT> CompletableFuture<RespT> invoke(String tenantId,
                                                          @Nullable String desiredServerId,
                                                          ReqT req,
                                                          MethodDescriptor<ReqT, RespT> methodDesc) {
        return invoke(tenantId, desiredServerId, req, emptyMap(), methodDesc);
    }

    <ReqT, RespT> CompletableFuture<RespT> invoke(String tenantId,
                                                  @Nullable String desiredServerId,
                                                  ReqT req,
                                                  Map<String, String> metadata,
                                                  MethodDescriptor<ReqT, RespT> methodDesc);

    /**
     * Create a caller-managed request-response pipeline.
     *
     * @param tenantId        the tenant id
     * @param desiredServerId the desired server id
     * @param wchKey          key for calculating weighted consistent hash
     * @param metadata        associated with the pipeline
     * @param methodDesc      the method descriptor
     * @return a request pipeline
     */
    default <ReqT, RespT> IRequestPipeline<ReqT, RespT> createRequestPipeline(String tenantId,
                                                                              @Nullable String desiredServerId,
                                                                              @Nullable String wchKey,
                                                                              Map<String, String> metadata,
                                                                              MethodDescriptor<ReqT, RespT> methodDesc) {
        return createRequestPipeline(tenantId, desiredServerId, wchKey, () -> metadata, methodDesc);
    }

    /**
     * Create a caller-managed request-response pipeline with specified executor.
     *
     * @param tenantId         the tenant id
     * @param desiredServerId  the desired server id
     * @param wchKey           key for calculating weighted consistent hash
     * @param metadataSupplier supply the metadata of the pipeline
     * @param methodDesc       the method descriptor
     * @return a request pipeline
     */
    <ReqT, RespT> IRequestPipeline<ReqT, RespT> createRequestPipeline(String tenantId,
                                                                      @Nullable String desiredServerId,
                                                                      @Nullable String wchKey,
                                                                      Supplier<Map<String, String>> metadataSupplier,
                                                                      MethodDescriptor<ReqT, RespT> methodDesc);

    /**
     * Create a caller-managed auto-rebalanced bi-directional message stream with at-most-once delivery guarantee.
     *
     * @param tenantId        the tenant id
     * @param desiredServerId the desired server id
     * @param wchKey          key for calculating weighted consistent hash
     * @param metadata        the metadata of the message stream
     * @param methodDesc      the method descriptor
     * @return a message stream
     */
    default <MsgT, AckT> IMessageStream<MsgT, AckT> createMessageStream(String tenantId,
                                                                        @Nullable String desiredServerId,
                                                                        @Nullable String wchKey,
                                                                        Map<String, String> metadata,
                                                                        MethodDescriptor<AckT, MsgT> methodDesc) {
        return createMessageStream(tenantId, desiredServerId, wchKey, () -> metadata, methodDesc);
    }

    /**
     * Create a caller-managed auto-rebalanced bi-directional message stream with at-most-once delivery guarantee.
     *
     * @param tenantId         the tenant id
     * @param desiredServerId  the desired server id
     * @param wchKey           key for calculating weighted consistent hash
     * @param metadataSupplier supply the metadata of the pipeline
     * @param methodDesc       the method descriptor
     * @return a message stream
     */
    <MsgT, AckT> IMessageStream<MsgT, AckT> createMessageStream(String tenantId,
                                                                @Nullable String desiredServerId,
                                                                @Nullable String wchKey,
                                                                Supplier<Map<String, String>> metadataSupplier,
                                                                MethodDescriptor<AckT, MsgT> methodDesc);

    /**
     * Close the client.
     */
    void stop();

    /**
     * The interface for managed request-response pipeline.
     *
     * @param <ReqT>  the request type
     * @param <RespT> the response type
     */
    interface IRequestPipeline<ReqT, RespT> {
        boolean isClosed();

        CompletableFuture<RespT> invoke(ReqT req);

        void close();
    }


    /**
     * The interface for managed bi-di message stream, which will automatically handle load balance change.
     *
     * @param <MsgT> the message received from server
     * @param <AckT> the ack send to server
     */
    interface IMessageStream<MsgT, AckT> {
        boolean isClosed();

        /**
         * Send ack to server.
         *
         * @param ack the ack
         */
        void ack(AckT ack);

        /**
         * Register a message consumer.
         *
         * @param consumer the consumer
         */
        void onMessage(Consumer<MsgT> consumer);

        /**
         * Register a retarget event consumer.
         *
         * @param consumer the consumer
         */
        void onRetarget(Consumer<Long> consumer);

        /**
         * Close the stream.
         */
        void close();
    }
}
