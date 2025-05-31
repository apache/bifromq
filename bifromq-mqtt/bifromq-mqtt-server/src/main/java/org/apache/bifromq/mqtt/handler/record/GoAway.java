/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.mqtt.handler.record;

import org.apache.bifromq.plugin.eventcollector.Event;
import io.netty.handler.codec.mqtt.MqttMessage;

public record GoAway(MqttMessage farewell, boolean rightNow, Event<?>... reasons) {
    public GoAway(Event<?>... reasons) {
        this(null, false, reasons);
    }

    public GoAway(MqttMessage farewell, Event<?>... reasons) {
        this(farewell, false, reasons);
    }

    public static GoAway now(Event<?>... reasons) {
        return new GoAway(null, true, reasons);
    }

    public static GoAway now(MqttMessage farewell, Event<?>... reasons) {
        return new GoAway(farewell, true, reasons);
    }
}
