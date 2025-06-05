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

package org.apache.bifromq.baserpc.client.interceptor;

import static org.apache.bifromq.baserpc.MetadataKeys.CUSTOM_METADATA_META_KEY;
import static org.apache.bifromq.baserpc.MetadataKeys.TENANT_ID_META_KEY;

import org.apache.bifromq.baserpc.RPCContext;
import org.apache.bifromq.baserpc.client.loadbalancer.Constants;
import org.apache.bifromq.baserpc.proto.PipelineMetadata;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TenantAwareClientInterceptor implements ClientInterceptor {
    private final String serviceUniqueName;

    public TenantAwareClientInterceptor() {
        this(null);
    }

    // used in in-process mode
    public TenantAwareClientInterceptor(String serviceUniqueName) {
        this.serviceUniqueName = serviceUniqueName;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                               CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                headers.put(TENANT_ID_META_KEY, RPCContext.TENANT_ID_CTX_KEY.get());
                if (RPCContext.DESIRED_SERVER_ID_CTX_KEY.get() != null) {
                    headers.put(Constants.DESIRED_SERVER_META_KEY, RPCContext.DESIRED_SERVER_ID_CTX_KEY.get());
                }
                if (RPCContext.CUSTOM_METADATA_CTX_KEY.get() != null) {
                    headers.put(CUSTOM_METADATA_META_KEY, PipelineMetadata.newBuilder()
                        .putAllEntry(RPCContext.CUSTOM_METADATA_CTX_KEY.get()).build().toByteArray());
                }
                super.start(responseListener, headers);
            }
        };
    }
}
