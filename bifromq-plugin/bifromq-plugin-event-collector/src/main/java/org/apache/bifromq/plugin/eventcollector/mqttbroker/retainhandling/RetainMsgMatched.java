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

package org.apache.bifromq.plugin.eventcollector.mqttbroker.retainhandling;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.bifromq.plugin.eventcollector.EventType;
import org.apache.bifromq.type.QoS;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
@ToString(callSuper = true)
public final class RetainMsgMatched extends RetainEvent<RetainMsgMatched> {
    private String topicFilter;

    private QoS qos;

    @Override
    public EventType type() {
        return EventType.RETAIN_MSG_MATCHED;
    }

    @Override
    public void clone(RetainMsgMatched orig) {
        super.clone(orig);
        this.topicFilter = orig.topicFilter;
        this.qos = orig.qos;
    }
}
