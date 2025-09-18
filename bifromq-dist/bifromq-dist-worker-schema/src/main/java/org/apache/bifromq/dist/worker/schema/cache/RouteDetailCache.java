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

package org.apache.bifromq.dist.worker.schema.cache;

import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.FLAG_NORMAL;
import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.FLAG_ORDERED;
import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.FLAG_UNORDERED;
import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.SCHEMA_VER;
import static org.apache.bifromq.util.BSUtil.toShort;
import static org.apache.bifromq.util.TopicConst.DELIMITER;
import static org.apache.bifromq.util.TopicConst.ORDERED_SHARE;
import static org.apache.bifromq.util.TopicConst.UNORDERED_SHARE;
import static org.apache.bifromq.util.TopicUtil.parse;
import static org.apache.bifromq.util.TopicUtil.unescape;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Interner;
import com.google.protobuf.ByteString;
import org.apache.bifromq.dist.worker.schema.RouteDetail;
import org.apache.bifromq.type.RouteMatcher;

public class RouteDetailCache {
    private static final Interner<String> TENANTID_INTERNER = Interner.newWeakInterner();
    private static final Interner<String> MQTT_TOPIC_FILTER_INTERNER = Interner.newWeakInterner();
    private static final Interner<ByteString> ROUTE_KEY_INTERNER = Interner.newWeakInterner();
    private static final Interner<RouteMatcher> ROUTE_MATCHER_INTERNER = Interner.newWeakInterner();
    private static final Cache<String, RouteMatcher> ROUTE_MATCHER_CACHE = Caffeine.newBuilder().weakKeys().build();
    private static final Cache<ByteString, RouteDetail> ROUTE_DETAIL_CACHE = Caffeine.newBuilder().weakKeys().build();

    public static RouteDetail get(ByteString routeKey) {
        // <VER><LENGTH_PREFIX_TENANT_ID><ESCAPED_TOPIC_FILTER><SEP><BUCKET_BYTE><FLAG_BYTE><LENGTH_SUFFIX_RECEIVER_BYTES>
        routeKey = ROUTE_KEY_INTERNER.intern(routeKey);
        return ROUTE_DETAIL_CACHE.get(routeKey, k -> {
            short tenantIdLen = tenantIdLen(k);
            int tenantIdStartIdx = SCHEMA_VER.size() + Short.BYTES;
            int escapedTopicFilterStartIdx = tenantIdStartIdx + tenantIdLen;
            int receiverBytesLen = receiverBytesLen(k);
            int receiverBytesStartIdx = k.size() - Short.BYTES - receiverBytesLen;
            int receiverBytesEndIdx = k.size() - Short.BYTES;
            int flagByteIdx = receiverBytesStartIdx - 1;
            int separatorBytesIdx = flagByteIdx - 1 - 2; // 2 bytes separator
            String receiverInfo = k.substring(receiverBytesStartIdx, receiverBytesEndIdx).toStringUtf8();
            byte flag = k.byteAt(flagByteIdx);

            String tenantId = TENANTID_INTERNER.intern(
                k.substring(tenantIdStartIdx, escapedTopicFilterStartIdx).toStringUtf8());
            String escapedTopicFilter = k.substring(escapedTopicFilterStartIdx, separatorBytesIdx)
                .toStringUtf8();
            switch (flag) {
                case FLAG_NORMAL -> {
                    String mqttTopicFilter = MQTT_TOPIC_FILTER_INTERNER.intern(unescape(escapedTopicFilter));
                    RouteMatcher matcher = ROUTE_MATCHER_CACHE.get(mqttTopicFilter,
                        t -> ROUTE_MATCHER_INTERNER.intern(RouteMatcher.newBuilder()
                            .setType(RouteMatcher.Type.Normal)
                            .addAllFilterLevel(parse(escapedTopicFilter, true))
                            .setMqttTopicFilter(t)
                            .build()));
                    return new RouteDetail(tenantId, matcher, receiverInfo); // receiverInfo is the receiverUrl
                }
                case FLAG_UNORDERED -> {
                    String mqttTopicFilter = MQTT_TOPIC_FILTER_INTERNER.intern(
                        UNORDERED_SHARE + DELIMITER + receiverInfo + DELIMITER + unescape(escapedTopicFilter));
                    RouteMatcher matcher = ROUTE_MATCHER_CACHE.get(mqttTopicFilter,
                        t -> ROUTE_MATCHER_INTERNER.intern(RouteMatcher.newBuilder()
                            .setType(RouteMatcher.Type.UnorderedShare)
                            .addAllFilterLevel(parse(escapedTopicFilter, true))
                            .setGroup(receiverInfo) // receiverInfo is the group name
                            .setMqttTopicFilter(t)
                            .build()));
                    return new RouteDetail(tenantId, matcher, null);
                }
                case FLAG_ORDERED -> {
                    String mqttTopicFilter = MQTT_TOPIC_FILTER_INTERNER.intern(
                        ORDERED_SHARE + DELIMITER + receiverInfo + DELIMITER + unescape(escapedTopicFilter));
                    RouteMatcher matcher = ROUTE_MATCHER_CACHE.get(mqttTopicFilter, t -> {
                        RouteMatcher m = RouteMatcher.newBuilder()
                            .setType(RouteMatcher.Type.OrderedShare)
                            .addAllFilterLevel(parse(escapedTopicFilter, true))
                            .setGroup(receiverInfo) // group name
                            .setMqttTopicFilter(t)
                            .build();
                        return ROUTE_MATCHER_INTERNER.intern(m);
                    });
                    return new RouteDetail(tenantId, matcher, null);
                }
                default -> throw new UnsupportedOperationException("Unknown route type: " + flag);
            }
        });
    }

    private static short tenantIdLen(ByteString routeKey) {
        return toShort(routeKey.substring(SCHEMA_VER.size(), SCHEMA_VER.size() + Short.BYTES));
    }

    private static short receiverBytesLen(ByteString routeKey) {
        return toShort(routeKey.substring(routeKey.size() - Short.BYTES));
    }
}
