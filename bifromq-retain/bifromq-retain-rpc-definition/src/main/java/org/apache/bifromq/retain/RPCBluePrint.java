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

package org.apache.bifromq.retain;

import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.retain.rpc.proto.RetainServiceGrpc;

public class RPCBluePrint {
    public static final BluePrint INSTANCE = BluePrint.builder()
        .serviceDescriptor(RetainServiceGrpc.getServiceDescriptor())
        .methodSemantic(RetainServiceGrpc.getRetainMethod(), BluePrint.WRUnaryMethod.getInstance())
        .methodSemantic(RetainServiceGrpc.getMatchMethod(), BluePrint.WRUnaryMethod.getInstance())
        .methodSemantic(RetainServiceGrpc.getExpireAllMethod(), BluePrint.WRUnaryMethod.getInstance())
        .build();
}
