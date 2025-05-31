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

package org.apache.bifromq.retain.server.scheduler;

import org.apache.bifromq.retain.rpc.proto.BatchRetainRequest;
import org.apache.bifromq.retain.rpc.proto.RetainMessage;
import org.apache.bifromq.retain.rpc.proto.RetainParam;
import org.apache.bifromq.retain.rpc.proto.RetainRequest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class BatchRetainCallHelper {
    static BatchRetainRequest makeBatch(Iterator<RetainRequest> retainRequestIterator) {
        Map<String, RetainParam.Builder> retainMsgPackBuilders = new HashMap<>(128);
        retainRequestIterator.forEachRemaining(request ->
            retainMsgPackBuilders.computeIfAbsent(request.getPublisher().getTenantId(), k -> RetainParam.newBuilder())
                .putTopicMessages(request.getTopic(), RetainMessage.newBuilder()
                    .setMessage(request.getMessage().toBuilder().setIsRetained(true).build())
                    .setPublisher(request.getPublisher())
                    .build()));
        long reqId = System.nanoTime();
        BatchRetainRequest.Builder reqBuilder = BatchRetainRequest.newBuilder().setReqId(reqId);
        retainMsgPackBuilders.forEach((tenantId, retainMsgPackBuilder) ->
            reqBuilder.putParams(tenantId, retainMsgPackBuilder.build()));
        return reqBuilder.build();
    }
}
