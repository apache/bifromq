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

package org.apache.bifromq.baserpc.server;

import org.apache.bifromq.baserpc.RPCContext;
import org.apache.bifromq.baserpc.metrics.IRPCMeter;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractStreamObserver<InT, OutT> implements StreamObserver<InT> {
    protected final String tenantId;
    protected final Map<String, String> metadata;
    protected final ServerCallStreamObserver<OutT> responseObserver;
    protected final IRPCMeter.IRPCMethodMeter meter;

    protected AbstractStreamObserver(StreamObserver<OutT> responseObserver) {
        tenantId = RPCContext.TENANT_ID_CTX_KEY.get();
        metadata = RPCContext.CUSTOM_METADATA_CTX_KEY.get();
        meter = RPCContext.METER_KEY_CTX_KEY.get();
        this.responseObserver = (ServerCallStreamObserver<OutT>) responseObserver;
        log.trace("Pipeline@{} created: tenantId={}", hashCode(), tenantId);
    }

    public final Map<String, String> metadata() {
        return metadata;
    }

    public final String metadata(String key) {
        return metadata.get(key);
    }

    public final boolean hasMetadata(String key) {
        return metadata.containsKey(key);
    }

    public abstract void close();
}
