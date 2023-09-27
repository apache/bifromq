/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
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

package com.baidu.bifromq.apiserver.http.handler;

import com.baidu.bifromq.apiserver.Headers;
import com.baidu.bifromq.basehlc.HLC;
import com.baidu.bifromq.type.QoS;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.util.HashMap;
import java.util.Map;

import static com.baidu.bifromq.apiserver.Headers.*;

public final class HTTPHeaderUtils {
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

    public static int getRequiredSubBrokerId(HttpRequest req) {
        return Integer.parseInt(getHeader(HEADER_SUBBROKER_ID, req, true));
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
            throw new NullPointerException("header not found: " + header);
        }
        return headerText;
    }

    public static String getDelivererKey(Headers header, HttpRequest req, int subBrokerId) {
        String delivererKey = req.headers().get(header.header);
        if (delivererKey == null && subBrokerId != 0 && subBrokerId != 1) {
            throw new NullPointerException("header not found: " + header);
        }
        return delivererKey;
    }

    public static boolean getRetain(HttpRequest req) {
        return Boolean.valueOf(getHeader(HEADER_RETAIN, req, false));
    }
}
