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

import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basekv.store.proto.BaseKVStoreServiceGrpc;
import org.apache.bifromq.baserpc.BluePrint;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import org.testng.annotations.Test;

public class RPCBluePrintTest {
    @Test
    public void buildScopedServiceDescriptor() {
        String clusterId = "testCluster";
        BluePrint bluePrint = RPCBluePrint.build(clusterId);
        assertTrue(bluePrint.serviceDescriptor().getName().startsWith(clusterId));
        for (String name : bluePrint.allMethods()) {
            assertTrue(name.startsWith(clusterId));
        }
    }

    @Test
    public void scopeServiceDefinition() {
        String clusterId = "testCluster";
        ServerServiceDefinition orig = new BaseKVStoreServiceGrpc.BaseKVStoreServiceImplBase() {
        }.bindService();
        ServerServiceDefinition scoped = RPCBluePrint.scope(orig, clusterId);
        assertTrue(scoped.getServiceDescriptor().getName().startsWith(clusterId));
        for (ServerMethodDefinition<?, ?> methodDefinition : scoped.getMethods()) {
            assertTrue(methodDefinition.getMethodDescriptor().getServiceName().startsWith(clusterId));
            assertTrue(methodDefinition.getMethodDescriptor().getFullMethodName().startsWith(clusterId));
        }
    }
}
