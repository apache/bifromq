/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.baserpc.metrics;

import org.apache.bifromq.baserpc.BluePrint;
import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import java.util.HashMap;
import java.util.Map;

public class RPCMeter implements IRPCMeter {
    private final Map<String, IRPCMethodMeter> meterMap = new HashMap<>();

    public RPCMeter(ServiceDescriptor serviceDescriptor, BluePrint bluePrint) {
        for (MethodDescriptor<?, ?> methodDesc : serviceDescriptor.getMethods()) {
            String serviceName = serviceDescriptor.getName();
            BluePrint.MethodSemantic semantic = bluePrint.semantic(methodDesc.getFullMethodName());
            if (semantic != null) {
                meterMap.put(methodDesc.getFullMethodName(), new RPCMethodMeter(serviceName, methodDesc, semantic));
            }
        }
    }

    @Override
    public IRPCMethodMeter get(MethodDescriptor<?, ?> methodDescriptor) {
        return meterMap.get(methodDescriptor.getFullMethodName());
    }
}
