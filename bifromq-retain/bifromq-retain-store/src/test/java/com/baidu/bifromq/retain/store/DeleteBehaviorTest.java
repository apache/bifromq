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

package com.baidu.bifromq.retain.store;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.baidu.bifromq.plugin.settingprovider.Setting;
import com.baidu.bifromq.retain.rpc.proto.MatchResult;
import com.baidu.bifromq.retain.rpc.proto.RetainResult;
import org.testng.annotations.Test;

public class DeleteBehaviorTest extends RetainStoreTest {
    @Test(groups = "integration")
    public void deleteFromEmptyRetainSet() {
        String tenantId = "tenantA";
        String topic = "/a/b/c";
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(10);
        // empty payload signal deletion
        RetainResult reply = requestRetain(tenantId, message(topic, ""));
        assertEquals(reply, RetainResult.CLEARED);
    }

    @Test(groups = "integration")
    public void deleteNonExisting() {
        String tenantId = "tenantA";
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(10);
        // empty payload signal deletion
        assertEquals(requestRetain(tenantId, message("/a/b/c", "hello")), RetainResult.RETAINED);

        assertEquals(requestRetain(tenantId, message("/a", "")), RetainResult.CLEARED);
    }

    @Test(groups = "integration")
    public void deleteNonExpired() {
        String tenantId = "tenantA";
        String topic = "/a/b/c";
        when(settingProvider.provide(Setting.RetainedTopicLimit, tenantId)).thenReturn(10);

        // empty payload signal deletion
        assertEquals(requestRetain(tenantId, message(topic, "hello")), RetainResult.RETAINED);

        assertEquals(requestRetain(tenantId, message(topic, "")), RetainResult.CLEARED);

        MatchResult matchReply = requestMatch(tenantId, topic, 10);
        assertEquals(matchReply.getOk().getMessagesCount(), 0);
    }
}
