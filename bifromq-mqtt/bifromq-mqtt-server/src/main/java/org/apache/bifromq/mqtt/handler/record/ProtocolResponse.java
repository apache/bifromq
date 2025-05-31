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

public record ProtocolResponse(MqttMessage message, Action action, Event<?>... reasons) {
    public enum Action {
        NoResponse,
        Response,
        GoAway,
        GoAwayNow,
        ResponseAndGoAway,
        ResponseAndGoAwayNow
    }

    public static ProtocolResponse responseNothing(Event<?>... reasons) {
        return new ProtocolResponse(null, Action.NoResponse, reasons);
    }

    public static ProtocolResponse response(MqttMessage message, Event<?>... reasons) {
        return new ProtocolResponse(message, Action.Response, reasons);
    }

    public static ProtocolResponse goAway(Event<?>... reasons) {
        return new ProtocolResponse(null, Action.GoAway, reasons);
    }

    public static ProtocolResponse goAwayNow(Event<?>... reasons) {
        return new ProtocolResponse(null, Action.GoAwayNow, reasons);
    }

    public static ProtocolResponse farewell(MqttMessage farewell, Event<?>... reasons) {
        return new ProtocolResponse(farewell, Action.ResponseAndGoAway, reasons);
    }

    public static ProtocolResponse farewellNow(MqttMessage farewell, Event<?>... reasons) {
        return new ProtocolResponse(farewell, Action.ResponseAndGoAwayNow, reasons);
    }
}
