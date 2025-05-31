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

package org.apache.bifromq.inbox;

import static org.apache.bifromq.inbox.util.InboxServiceUtil.getDelivererKey;

import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.inbox.rpc.proto.AttachRequest;
import org.apache.bifromq.inbox.rpc.proto.CommitRequest;
import org.apache.bifromq.inbox.rpc.proto.DeleteRequest;
import org.apache.bifromq.inbox.rpc.proto.DetachRequest;
import org.apache.bifromq.inbox.rpc.proto.ExistRequest;
import org.apache.bifromq.inbox.rpc.proto.InboxServiceGrpc;
import org.apache.bifromq.inbox.rpc.proto.SendLWTRequest;
import org.apache.bifromq.inbox.rpc.proto.SubRequest;
import org.apache.bifromq.inbox.rpc.proto.UnsubRequest;
import org.apache.bifromq.plugin.subbroker.CheckRequest;

public class RPCBluePrint {
    public static final BluePrint INSTANCE = BluePrint.builder()
        .serviceDescriptor(InboxServiceGrpc.getServiceDescriptor())
        // inbox related rpc must be routed using WCH mode
        // broker client rpc
        .methodSemantic(InboxServiceGrpc.getReceiveMethod(), BluePrint.WCHPipelineUnaryMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getFetchMethod(), BluePrint.WCHStreamingMethod.getInstance())
        .methodSemantic(InboxServiceGrpc.getCheckSubscriptionsMethod(), BluePrint.WCHUnaryMethod.<CheckRequest>builder()
            .keyHashFunc(CheckRequest::getDelivererKey).build())
        // both broker and reader client rpc
        .methodSemantic(InboxServiceGrpc.getExistMethod(), BluePrint.WCHUnaryMethod.<ExistRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        // reader client rpc
        .methodSemantic(InboxServiceGrpc.getAttachMethod(), BluePrint.WCHUnaryMethod.<AttachRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getClient().getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getDetachMethod(), BluePrint.WCHUnaryMethod.<DetachRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getClient().getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getSubMethod(), BluePrint.WCHUnaryMethod.<SubRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getUnsubMethod(), BluePrint.WCHUnaryMethod.<UnsubRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getSendLWTMethod(), BluePrint.WCHUnaryMethod.<SendLWTRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getDeleteMethod(), BluePrint.WCHUnaryMethod.<DeleteRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId())).build())
        .methodSemantic(InboxServiceGrpc.getCommitMethod(), BluePrint.WCHUnaryMethod
            .<CommitRequest>builder()
            .keyHashFunc(request -> getDelivererKey(request.getTenantId(), request.getInboxId()))
            .build())
        // expire all
        .methodSemantic(InboxServiceGrpc.getExpireAllMethod(), BluePrint.WRUnaryMethod.getInstance())
        .build();
}
