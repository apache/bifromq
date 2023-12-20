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

package com.baidu.bifromq.plugin.eventcollector.distservice;

import com.baidu.bifromq.plugin.eventcollector.Event;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.type.SubInfo;
import com.baidu.bifromq.type.TopicMessagePack;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
@ToString(callSuper = true)
public final class DeliverNoInbox extends Event<DeliverNoInbox> {
    private int brokerId;
    private String delivererKey;
    private SubInfo subInfo;
    private TopicMessagePack messages;

    @Override
    public EventType type() {
        return EventType.DELIVER_NO_INBOX;
    }

    @Override
    public void clone(DeliverNoInbox orig) {
        super.clone(orig);
        this.brokerId = orig.brokerId;
        this.delivererKey = orig.delivererKey;
        this.subInfo = orig.subInfo;
        this.messages = orig.messages;
    }
}
