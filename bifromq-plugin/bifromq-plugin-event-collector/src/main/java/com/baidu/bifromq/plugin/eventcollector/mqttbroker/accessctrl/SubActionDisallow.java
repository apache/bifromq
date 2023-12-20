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

package com.baidu.bifromq.plugin.eventcollector.mqttbroker.accessctrl;

import com.baidu.bifromq.plugin.eventcollector.ClientEvent;
import com.baidu.bifromq.plugin.eventcollector.EventType;
import com.baidu.bifromq.type.QoS;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
@ToString(callSuper = true)
public final class SubActionDisallow extends ClientEvent<SubActionDisallow> {
    private String topicFilter;
    private QoS qos;

    @Override
    public EventType type() {
        return EventType.SUB_ACTION_DISALLOW;
    }

    @Override
    public void clone(SubActionDisallow orig) {
        super.clone(orig);
        this.topicFilter = orig.topicFilter;
        this.qos = orig.qos;
    }
}
