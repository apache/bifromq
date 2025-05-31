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

package org.apache.bifromq.plugin.eventcollector.distservice;

import org.apache.bifromq.plugin.eventcollector.Event;
import org.apache.bifromq.plugin.eventcollector.EventType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
@ToString(callSuper = true)
public final class UnmatchError extends Event<UnmatchError> {
    private long reqId;
    private String topicFilter;
    private String tenantId;
    private String receiverId;
    private int subBrokerId;
    private String delivererKey;
    private String reason;

    @Override
    public EventType type() {
        return EventType.UNMATCH_ERROR;
    }

    @Override
    public void clone(UnmatchError orig) {
        super.clone(orig);
        this.reqId = orig.reqId;
        this.topicFilter = orig.topicFilter;
        this.tenantId = orig.tenantId;
        this.receiverId = orig.receiverId;
        this.subBrokerId = orig.subBrokerId;
        this.delivererKey = orig.delivererKey;
        this.reason = orig.reason;
    }
}
