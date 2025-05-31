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

package org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling;

import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.type.QoS;
import java.nio.ByteBuffer;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
@ToString(callSuper = true)
public final class MsgRetainedError extends RetainEvent<MsgRetainedError> {
    private String topic;

    private boolean isLastWill;

    private QoS qos;

    private ByteBuffer payload;

    private int size;

    private String reason;

    @Override
    public EventType type() {
        return EventType.MSG_RETAINED_ERROR;
    }

    @Override
    public void clone(MsgRetainedError orig) {
        super.clone(orig);
        this.topic = orig.topic;
        this.isLastWill = orig.isLastWill;
        this.qos = orig.qos;
        this.payload = orig.payload;
        this.size = orig.size;
        this.reason = orig.reason;
    }
}
