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

package org.apache.bifromq.plugin.eventcollector.distservice;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.EventType;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
@ToString(callSuper = true)
public final class GroupFanoutThrottled extends Event<GroupFanoutThrottled> {
    private String tenantId;
    private String topic;
    private int maxCount;

    @Override
    public EventType type() {
        return EventType.GROUP_FANOUT_THROTTLED;
    }

    @Override
    public void clone(GroupFanoutThrottled orig) {
        super.clone(orig);
        this.tenantId = orig.tenantId;
        this.topic = orig.topic;
        this.maxCount = orig.maxCount;
    }
}
