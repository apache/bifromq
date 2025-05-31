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

package org.apache.bifromq.apiserver.http.handler;

import static org.apache.bifromq.apiserver.Headers.HEADER_CLIENT_META_PREFIX;
import static org.apache.bifromq.apiserver.Headers.HEADER_REQ_ID;
import static org.apache.bifromq.apiserver.Headers.HEADER_SUB_QOS;

import org.apache.bifromq.apiserver.Headers;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.type.QoS;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.util.HashMap;
import java.util.Map;

public final class HeaderUtils {
    public static long getOptionalReqId(FullHttpRequest req) {
        String headerText = getHeader(HEADER_REQ_ID, req, false);
        if (headerText == null) {
            return HLC.INST.get();
        }
        try {
            return Long.parseLong(headerText);
        } catch (Throwable e) {
            return HLC.INST.get();
        }
    }

    public static QoS getRequiredSubQoS(HttpRequest req) {
        String subQoS = getHeader(HEADER_SUB_QOS, req, true);
        QoS qos = QoS.forNumber(Integer.parseInt(subQoS));
        if (qos == null) {
            throw new IllegalArgumentException("Invalid sub qos: " + subQoS);
        }
        return qos;
    }

    public static Map<String, String> getClientMeta(HttpRequest req) {
        Map<String, String> clientMeta = new HashMap<>();
        for (Map.Entry<String, String> entry : req.headers()) {
            if (entry.getKey().startsWith(HEADER_CLIENT_META_PREFIX.header)) {
                clientMeta.put(entry.getKey().substring(HEADER_CLIENT_META_PREFIX.header.length()), entry.getValue());
            }
        }
        return clientMeta;
    }

    public static String getHeader(Headers header, HttpRequest req, boolean required) {
        String headerText = req.headers().get(header.header);
        if (headerText == null && required) {
            throw new IllegalArgumentException("header not found: " + header);
        }
        return headerText;
    }
}
