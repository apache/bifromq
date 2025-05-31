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

package org.apache.bifromq.plugin.eventcollector.mqttbroker.pushhandling;

import org.apache.bifromq.plugin.eventcollector.EventType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
@ToString(callSuper = true)
public final class QoS1Confirmed extends PushEvent<QoS1Confirmed> {
    private int messageId;

    /**
     * Whether the qos1 message has been delivered to client, or dropped due to max resend times
     *
     * @return
     */
    private boolean delivered;

    @Override
    public EventType type() {
        return EventType.QOS1_CONFIRMED;
    }

    @Override
    public void clone(QoS1Confirmed orig) {
        super.clone(orig);
        this.messageId = orig.messageId;
        this.delivered = orig.delivered;
    }
}
