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

package org.apache.bifromq.basekv;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getBootstrapMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getChangeReplicaConfigMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getExecuteMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getLinearizedQueryMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getMergeMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getQueryMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getRecoverMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getSplitMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getTransferLeadershipMethod;
import static org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc.getZombieQuitMethod;

import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import java.util.HashMap;
import java.util.Map;
import org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc;
import org.apache.bifromq.baserpc.BluePrint;

public class RPCBluePrint {
    public static ServerServiceDefinition scope(ServerServiceDefinition definition, String clusterId) {
        String serviceName = toScopedServiceName(clusterId, definition.getServiceDescriptor().getName());
        ServerServiceDefinition.Builder builder = ServerServiceDefinition.builder(serviceName);
        for (ServerMethodDefinition origMethodDef : definition.getMethods()) {
            MethodDescriptor methodDesc = origMethodDef.getMethodDescriptor().toBuilder()
                .setFullMethodName(generateFullMethodName(serviceName,
                    origMethodDef.getMethodDescriptor().getBareMethodName()))
                .build();
            builder.addMethod(methodDesc, origMethodDef.getServerCallHandler());
        }
        return builder.build();
    }

    public static BluePrint build(String clusterId) {
        ServiceDescriptor orig = BaseKVStoreServiceGrpc.getServiceDescriptor();
        String serviceName = toScopedServiceName(clusterId, orig.getName());
        ServiceDescriptor.Builder builder = ServiceDescriptor.newBuilder(serviceName)
            .setSchemaDescriptor(orig.getSchemaDescriptor());

        Map<MethodDescriptor<?, ?>, MethodDescriptor<?, ?>> methodMap = new HashMap<>();
        for (MethodDescriptor<?, ?> origMethodDesc : orig.getMethods()) {
            MethodDescriptor<?, ?> scopedMethodDesc = origMethodDesc.toBuilder()
                .setFullMethodName(generateFullMethodName(serviceName, origMethodDesc.getBareMethodName()))
                .build();
            builder.addMethod(scopedMethodDesc);
            methodMap.put(origMethodDesc, scopedMethodDesc);
        }

        ServiceDescriptor serviceDesc = builder.build();

        return BluePrint.builder()
            .serviceDescriptor(serviceDesc)
            .methodSemantic(methodMap.get(getBootstrapMethod()), BluePrint.DDUnaryMethod.getInstance())
            .methodSemantic(methodMap.get(getRecoverMethod()), BluePrint.DDUnaryMethod.getInstance())
            .methodSemantic(methodMap.get(getZombieQuitMethod()), BluePrint.DDUnaryMethod.getInstance())
            .methodSemantic(methodMap.get(getChangeReplicaConfigMethod()), BluePrint.DDUnaryMethod.getInstance())
            .methodSemantic(methodMap.get(getSplitMethod()), BluePrint.DDUnaryMethod.getInstance())
            .methodSemantic(methodMap.get(getMergeMethod()), BluePrint.DDUnaryMethod.getInstance())
            .methodSemantic(methodMap.get(getTransferLeadershipMethod()), BluePrint.DDUnaryMethod.getInstance())
            .methodSemantic(methodMap.get(getExecuteMethod()), BluePrint.DDPipelineUnaryMethod.getInstance())
            .methodSemantic(methodMap.get(getQueryMethod()), BluePrint.DDPipelineUnaryMethod.getInstance())
            .methodSemantic(methodMap.get(getLinearizedQueryMethod()), BluePrint.DDPipelineUnaryMethod.getInstance())
            .build();
    }

    public static String toScopedFullMethodName(String clusterId, String fullMethodName) {
        return clusterId + "@" + fullMethodName;
    }

    private static String toScopedServiceName(String clusterId, String serviceName) {
        return clusterId + "@" + serviceName;
    }
}
