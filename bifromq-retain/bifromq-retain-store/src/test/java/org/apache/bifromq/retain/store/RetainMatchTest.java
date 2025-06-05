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

package org.apache.bifromq.retain.store;

import static com.google.common.collect.Sets.newHashSet;
import static org.testng.Assert.assertEquals;

import org.apache.bifromq.retain.rpc.proto.MatchResult;
import org.apache.bifromq.type.TopicMessage;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RetainMatchTest extends RetainStoreTest {
    private String tenantId;

    @BeforeMethod(alwaysRun = true)
    private void reset() {
        tenantId = "tenantA-" + System.nanoTime();
    }

    @Test(groups = "integration")
    public void wildcardTopicFilter() {
        TopicMessage message1 = message("/a/b/c", "hello");
        TopicMessage message2 = message("/a/b/", "hello");
        TopicMessage message3 = message("/c/", "hello");
        TopicMessage message4 = message("a", "hello");

        requestRetain(tenantId, message1);
        requestRetain(tenantId, message2);
        requestRetain(tenantId, message3);
        requestRetain(tenantId, message4);

        MatchResult matchReply = requestMatch(tenantId, "#", 10);
        assertEquals(matchReply.getMessagesCount(), 4);
        assertEquals(newHashSet(matchReply.getMessagesList()),
            newHashSet(message1, message2, message3, message4));

        matchReply = requestMatch(tenantId, "+", 10);
        assertEquals(matchReply.getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message4));

        matchReply = requestMatch(tenantId, "+/#", 10);
        assertEquals(matchReply.getMessagesCount(), 4);
        assertEquals(newHashSet(matchReply.getMessagesList()),
            newHashSet(message1, message2, message3, message4));

        matchReply = requestMatch(tenantId, "+/+/#", 10);
        assertEquals(matchReply.getMessagesCount(), 3);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message1, message2, message3));

        matchReply = requestMatch(tenantId, "+/+/+", 10);
        assertEquals(matchReply.getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message3));

        matchReply = requestMatch(tenantId, "/#", 10);
        assertEquals(matchReply.getMessagesCount(), 3);
        assertEquals(newHashSet(matchReply.getMessagesList()),
            newHashSet(message1, message2, message3));

        matchReply = requestMatch(tenantId, "/c/#", 10);
        assertEquals(matchReply.getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message3));

        matchReply = requestMatch(tenantId, "/a/+", 10);
        assertEquals(matchReply.getMessagesCount(), 0);

        matchReply = requestMatch(tenantId, "/a/#", 10);
        assertEquals(matchReply.getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message1, message2));

        matchReply = requestMatch(tenantId, "/a/+/+", 10);
        assertEquals(matchReply.getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message1, message2));

        matchReply = requestMatch(tenantId, "/a/+/#", 10);
        assertEquals(matchReply.getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message1, message2));

        matchReply = requestMatch(tenantId, "/+/b/", 10);
        assertEquals(matchReply.getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message2));

        matchReply = requestMatch(tenantId, "/+/b/#", 10);
        assertEquals(matchReply.getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message1, message2));

        matchReply = requestMatch(tenantId, "/a/b/c/#", 10);
        assertEquals(matchReply.getMessagesCount(), 1);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message1));

        matchReply = requestMatch(tenantId, "/a/b/#", 10);
        assertEquals(matchReply.getMessagesCount(), 2);
        assertEquals(newHashSet(matchReply.getMessagesList()), newHashSet(message1, message2));
    }

    @Test(groups = "integration")
    public void matchLimit() {
        TopicMessage message1 = message("/a/b/c", "hello");
        TopicMessage message2 = message("/a/b/", "hello");
        TopicMessage message3 = message("/c/", "hello");
        requestRetain(tenantId, message1);
        requestRetain(tenantId, message2);
        requestRetain(tenantId, message3);

        assertEquals(requestMatch(tenantId, "#", 0).getMessagesCount(), 0);
        assertEquals(requestMatch(tenantId, "#", 1).getMessagesCount(), 1);
    }
}
