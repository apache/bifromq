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

package org.apache.bifromq.dist.worker.schema;

import static org.apache.bifromq.util.BSUtil.toByteString;
import static org.apache.bifromq.util.BSUtil.toShort;
import static org.apache.bifromq.util.TopicConst.DELIMITER;
import static org.apache.bifromq.util.TopicConst.NUL;
import static org.apache.bifromq.util.TopicConst.ORDERED_SHARE;
import static org.apache.bifromq.util.TopicConst.UNORDERED_SHARE;
import static org.apache.bifromq.util.TopicUtil.parse;
import static org.apache.bifromq.util.TopicUtil.unescape;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.BSUtil;
import com.google.protobuf.ByteString;
import java.util.List;

/**
 * Utility for working with the data stored in dist worker.
 */
public class KVSchemaUtil {
    public static final ByteString SCHEMA_VER = ByteString.copyFrom(new byte[] {0x00});
    private static final int MAX_RECEIVER_BUCKETS = 0xFF; // one byte
    private static final byte FLAG_NORMAL = 0x01;
    private static final byte FLAG_UNORDERED = 0x02;
    private static final byte FLAG_ORDERED = 0x03;
    private static final ByteString SEPARATOR_BYTE = ByteString.copyFrom(new byte[] {0x00});
    private static final ByteString FLAG_NORMAL_VAL = ByteString.copyFrom(new byte[] {FLAG_NORMAL});
    private static final ByteString FLAG_UNORDERED_VAL = ByteString.copyFrom(new byte[] {FLAG_UNORDERED});
    private static final ByteString FLAG_ORDERED_VAL = ByteString.copyFrom(new byte[] {FLAG_ORDERED});

    public static String toReceiverUrl(MatchRoute route) {
        return toReceiverUrl(route.getBrokerId(), route.getReceiverId(), route.getDelivererKey());
    }

    public static String toReceiverUrl(int subBrokerId, String receiverId, String delivererKey) {
        return subBrokerId + NUL + receiverId + NUL + delivererKey;
    }

    public static Receiver parseReceiver(String receiverUrl) {
        String[] parts = receiverUrl.split(NUL);
        return new Receiver(Integer.parseInt(parts[0]), parts[1], parts[2]);
    }

    public static Matching buildMatchRoute(ByteString routeKey, ByteString routeValue) {
        RouteDetail routeDetail = parseRouteDetail(routeKey);
        try {
            if (routeDetail.matcher().getType() == RouteMatcher.Type.Normal) {
                return new NormalMatching(routeDetail.tenantId(),
                    routeDetail.matcher(),
                    routeDetail.receiverUrl(),
                    BSUtil.toLong(routeValue));
            }
            return new GroupMatching(routeDetail.tenantId(),
                routeDetail.matcher(),
                RouteGroup.parseFrom(routeValue).getMembersMap());
        } catch (Exception e) {
            throw new IllegalStateException("Unable to parse matching record", e);
        }
    }

    public static ByteString tenantBeginKey(String tenantId) {
        ByteString tenantIdBytes = copyFromUtf8(tenantId);
        return SCHEMA_VER.concat(toByteString((short) tenantIdBytes.size()).concat(tenantIdBytes));
    }

    public static ByteString tenantRouteStartKey(String tenantId, List<String> filterLevels) {
        ByteString key = tenantBeginKey(tenantId);
        for (String filterLevel : filterLevels) {
            key = key.concat(copyFromUtf8(filterLevel)).concat(SEPARATOR_BYTE);
        }
        return key.concat(SEPARATOR_BYTE);
    }

    private static ByteString tenantRouteBucketStartKey(String tenantId, List<String> filterLevels, byte bucket) {
        return tenantRouteStartKey(tenantId, filterLevels).concat(unsafeWrap(new byte[] {bucket}));
    }

    public static ByteString toNormalRouteKey(String tenantId, RouteMatcher routeMatcher, String receiverUrl) {
        assert routeMatcher.getType() == RouteMatcher.Type.Normal;
        return tenantRouteBucketStartKey(tenantId, routeMatcher.getFilterLevelList(), bucket(receiverUrl))
            .concat(FLAG_NORMAL_VAL)
            .concat(toReceiverBytes(receiverUrl));
    }

    public static ByteString toGroupRouteKey(String tenantId, RouteMatcher routeMatcher) {
        assert routeMatcher.getType() != RouteMatcher.Type.Normal;
        return tenantRouteBucketStartKey(tenantId, routeMatcher.getFilterLevelList(),
            bucket(routeMatcher.getGroup()))
            .concat(routeMatcher.getType() == RouteMatcher.Type.OrderedShare ? FLAG_ORDERED_VAL : FLAG_UNORDERED_VAL)
            .concat(toReceiverBytes(routeMatcher.getGroup()));
    }

    public static RouteDetail parseRouteDetail(ByteString routeKey) {
        // <VER><LENGTH_PREFIX_TENANT_ID><ESCAPED_TOPIC_FILTER><SEP><BUCKET_BYTE><FLAG_BYTE><LENGTH_SUFFIX_RECEIVER_BYTES>
        short tenantIdLen = tenantIdLen(routeKey);
        int tenantIdStartIdx = SCHEMA_VER.size() + Short.BYTES;
        int escapedTopicFilterStartIdx = tenantIdStartIdx + tenantIdLen;
        int receiverBytesLen = receiverBytesLen(routeKey);
        int receiverBytesStartIdx = routeKey.size() - Short.BYTES - receiverBytesLen;
        int receiverBytesEndIdx = routeKey.size() - Short.BYTES;
        int flagByteIdx = receiverBytesStartIdx - 1;
        int separatorBytesIdx = flagByteIdx - 1 - 2; // 2 bytes separator
        String receiverInfo = routeKey.substring(receiverBytesStartIdx, receiverBytesEndIdx).toStringUtf8();
        byte flag = routeKey.byteAt(flagByteIdx);

        String tenantId = routeKey.substring(tenantIdStartIdx, escapedTopicFilterStartIdx).toStringUtf8();
        String escapedTopicFilter =
            routeKey.substring(escapedTopicFilterStartIdx, separatorBytesIdx).toStringUtf8();
        switch (flag) {
            case FLAG_NORMAL -> {
                RouteMatcher matcher = RouteMatcher.newBuilder()
                    .setType(RouteMatcher.Type.Normal)
                    .addAllFilterLevel(parse(escapedTopicFilter, true))
                    .setMqttTopicFilter(unescape(escapedTopicFilter))
                    .build();
                return new RouteDetail(tenantId, matcher, receiverInfo);
            }
            case FLAG_UNORDERED -> {
                RouteMatcher matcher = RouteMatcher.newBuilder()
                    .setType(RouteMatcher.Type.UnorderedShare)
                    .addAllFilterLevel(parse(escapedTopicFilter, true))
                    .setGroup(receiverInfo)
                    .setMqttTopicFilter(
                        UNORDERED_SHARE + DELIMITER + receiverInfo + DELIMITER + unescape(escapedTopicFilter))
                    .build();
                return new RouteDetail(tenantId, matcher, null);
            }
            case FLAG_ORDERED -> {
                RouteMatcher matcher = RouteMatcher.newBuilder()
                    .setType(RouteMatcher.Type.OrderedShare)
                    .addAllFilterLevel(parse(escapedTopicFilter, true))
                    .setGroup(receiverInfo)
                    .setMqttTopicFilter(
                        ORDERED_SHARE + DELIMITER + receiverInfo + DELIMITER + unescape(escapedTopicFilter))
                    .build();
                return new RouteDetail(tenantId, matcher, null);
            }
            default -> throw new UnsupportedOperationException("Unknown route type: " + flag);
        }
    }

    private static short tenantIdLen(ByteString routeKey) {
        return toShort(routeKey.substring(SCHEMA_VER.size(), SCHEMA_VER.size() + Short.BYTES));
    }

    private static short receiverBytesLen(ByteString routeKey) {
        return toShort(routeKey.substring(routeKey.size() - Short.BYTES));
    }

    private static ByteString toReceiverBytes(String receiver) {
        ByteString b = copyFromUtf8(receiver);
        return b.concat(toByteString((short) b.size()));
    }

    private static byte bucket(String receiver) {
        int hash = receiver.hashCode();
        return (byte) ((hash ^ (hash >>> 16)) & MAX_RECEIVER_BUCKETS);
    }

    public record Receiver(int subBrokerId, String receiverId, String delivererKey) {

    }
}
