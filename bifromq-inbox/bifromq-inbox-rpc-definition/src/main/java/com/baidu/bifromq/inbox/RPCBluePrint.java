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

package com.baidu.bifromq.inbox;

import com.baidu.bifromq.baserpc.BluePrint;
import com.baidu.bifromq.inbox.rpc.proto.CommitRequest;
import com.baidu.bifromq.inbox.rpc.proto.HasInboxRequest;
import com.baidu.bifromq.inbox.rpc.proto.InboxServiceGrpc;

public class RPCBluePrint {
    public static final BluePrint INSTANCE = BluePrint.builder()
        .serviceDescriptor(InboxServiceGrpc.getServiceDescriptor())
        // broker client rpc
        .methodSemantic(InboxServiceGrpc.getReceiveMethod(), BluePrint.WCHPipelineUnaryMethod.getInstance())
        // both broker and reader client rpc
        .methodSemantic(InboxServiceGrpc.getHasInboxMethod(), BluePrint.WCHUnaryMethod.<HasInboxRequest>builder()
            .keyHashFunc(HasInboxRequest::getInboxId).build())
        // reader client rpc
        .methodSemantic(InboxServiceGrpc.getCreateInboxMethod(), BluePrint.WRUnaryMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getDeleteInboxMethod(), BluePrint.WRUnaryMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getTouchInboxMethod(), BluePrint.WRUnaryMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getSubMethod(), BluePrint.WRUnaryMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getUnsubMethod(), BluePrint.WRUnaryMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getFetchMethod(), BluePrint.WCHStreamingMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getCommitMethod(), BluePrint.WCHUnaryMethod
            .<CommitRequest>builder().keyHashFunc(CommitRequest::getInboxId).build())
        .build();
}
