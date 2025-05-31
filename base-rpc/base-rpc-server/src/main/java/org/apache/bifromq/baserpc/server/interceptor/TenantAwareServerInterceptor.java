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

package org.apache.bifromq.baserpc.server.interceptor;

import static org.apache.bifromq.baserpc.MetadataKeys.CUSTOM_METADATA_META_KEY;
import static org.apache.bifromq.baserpc.MetadataKeys.TENANT_ID_META_KEY;

import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.RPCContext;
import org.apache.bifromq.baserpc.metrics.RPCMeter;
import org.apache.bifromq.baserpc.proto.PipelineMetadata;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TenantAwareServerInterceptor implements ServerInterceptor {
    private static final ServerCall.Listener NOOP_LISTENER = new ServerCall.Listener<>() {
    };

    private final RPCMeter meter;

    public TenantAwareServerInterceptor(ServerServiceDefinition serviceDefinition, BluePrint bluePrint) {
        meter = new RPCMeter(serviceDefinition.getServiceDescriptor(), bluePrint);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next) {
        try {
            Context ctx = Context.current();
            assert headers.containsKey(TENANT_ID_META_KEY);
            String tenantId = headers.get(TENANT_ID_META_KEY);
            ctx = ctx.withValue(RPCContext.TENANT_ID_CTX_KEY, tenantId);
            if (headers.containsKey(CUSTOM_METADATA_META_KEY)) {
                PipelineMetadata metadata = PipelineMetadata.parseFrom(headers.get(CUSTOM_METADATA_META_KEY));
                ctx = ctx.withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, metadata.getEntryMap());
            } else {
                ctx = ctx.withValue(RPCContext.CUSTOM_METADATA_CTX_KEY, Collections.emptyMap());
            }
            ctx = ctx.withValue(RPCContext.METER_KEY_CTX_KEY, meter.get(call.getMethodDescriptor()));

            ServerCall.Listener<ReqT> listener = Contexts.interceptCall(ctx, call, headers, next);
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                @Override
                public void onHalfClose() {
                    try {
                        super.onHalfClose();
                    } catch (Exception e) {
                        log.error("Failed to execute server call.", e);
                        call.close(Status.INTERNAL.withCause(e).withDescription(e.getMessage()), headers);
                    }
                }
            };
        } catch (UnsupportedOperationException e) {
            log.error("Failed to determine traffic identifier from the call", e);
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid Client Certificate"), headers);
            return NOOP_LISTENER;
        } catch (Throwable e) {
            log.error("Failed to make server call", e);
            call.close(Status.INTERNAL.withDescription("Server handling request error"), headers);
            return NOOP_LISTENER;
        }
    }
}
