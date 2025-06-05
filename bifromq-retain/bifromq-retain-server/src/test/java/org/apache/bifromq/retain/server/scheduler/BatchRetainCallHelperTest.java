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

package org.apache.bifromq.retain.server.scheduler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.retain.rpc.proto.BatchRetainRequest;
import org.apache.bifromq.retain.rpc.proto.RetainMessage;
import org.apache.bifromq.retain.rpc.proto.RetainParam;
import org.apache.bifromq.retain.rpc.proto.RetainRequest;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.Message;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BatchRetainCallHelperTest {

    private RetainRequest retainRequest1;
    private RetainRequest retainRequest11;
    private RetainRequest retainRequest2;
    private Iterator<RetainRequest> retainRequestIterator;
    private Message message1;
    private Message message11;
    private Message message2;
    private ClientInfo publisher1;
    private ClientInfo publisher11;
    private ClientInfo publisher2;

    @BeforeMethod
    public void setUp() {
        message1 = Message.newBuilder().build();
        message11 = Message.newBuilder().build();
        message2 = Message.newBuilder().build();
        publisher1 = ClientInfo.newBuilder().setTenantId("tenant1").build();
        publisher11 = ClientInfo.newBuilder().setTenantId("tenant1").build();
        publisher2 = ClientInfo.newBuilder().setTenantId("tenant2").build();
        retainRequest1 = RetainRequest.newBuilder()
            .setTopic("topic1")
            .setMessage(message1)
            .setPublisher(publisher1)
            .build();
        retainRequest11 = RetainRequest.newBuilder()
            .setTopic("topic11")
            .setMessage(message11)
            .setPublisher(publisher11)
            .build();
        retainRequest2 = RetainRequest.newBuilder()
            .setTopic("topic2")
            .setMessage(message2)
            .setPublisher(publisher2)
            .build();

        // Create the iterator
        retainRequestIterator = Arrays.asList(retainRequest1, retainRequest11, retainRequest2).iterator();
    }

    @Test
    public void testMakeBatch() {
        BatchRetainRequest result = BatchRetainCallHelper.makeBatch(retainRequestIterator);

        assertNotNull(result);
        assertEquals(result.getParamsCount(), 2);

        Map<String, RetainParam> params = new HashMap<>(result.getParamsMap());
        assertTrue(params.containsKey("tenant1"));
        assertTrue(params.containsKey("tenant2"));

        RetainParam tenant1Param = params.get("tenant1");
        RetainParam tenant2Param = params.get("tenant2");

        assertTrue(tenant1Param.containsTopicMessages("topic1"));
        assertTrue(tenant1Param.containsTopicMessages("topic11"));
        assertTrue(tenant2Param.containsTopicMessages("topic2"));

        // Verify that the message was marked as retained
        RetainMessage tenant1Message = tenant1Param.getTopicMessagesOrThrow("topic1");
        RetainMessage tenant11Message = tenant1Param.getTopicMessagesOrThrow("topic11");
        RetainMessage tenant2Message = tenant2Param.getTopicMessagesOrThrow("topic2");

        assertTrue(tenant1Message.getMessage().getIsRetained());
        assertTrue(tenant11Message.getMessage().getIsRetained());
        assertTrue(tenant2Message.getMessage().getIsRetained());
    }
}