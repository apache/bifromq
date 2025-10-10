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

package org.apache.bifromq.inbox.server.scheduler;

import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;
import static org.testng.Assert.assertEquals;

import com.google.protobuf.ByteString;
import org.apache.bifromq.basekv.client.IBaseKVStoreClient;
import org.apache.bifromq.inbox.util.InboxServiceUtil;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.util.TopicUtil;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class InboxCheckSubSchedulerTest {

    @Test
    public void rangeKeyShouldRouteByInboxId() {
        // arrange
        String tenantId = "t1";
        String inboxId = "inboxA";
        long incarnation = 123456789L;
        String receiverId = InboxServiceUtil.receiverId(inboxId, incarnation);

        MatchInfo matchInfo = MatchInfo.newBuilder()
            .setMatcher(TopicUtil.from("a/b"))
            .setReceiverId(receiverId)
            .setIncarnation(incarnation)
            .build();

        CheckMatchInfo request = new CheckMatchInfo(tenantId, matchInfo);

        IBaseKVStoreClient storeClient = Mockito.mock(IBaseKVStoreClient.class);
        InboxCheckSubScheduler scheduler = new InboxCheckSubScheduler(storeClient);

        // act
        ByteString key = scheduler.rangeKey(request);

        // assert
        assertEquals(key, inboxStartKeyPrefix(tenantId, inboxId));
    }
}

