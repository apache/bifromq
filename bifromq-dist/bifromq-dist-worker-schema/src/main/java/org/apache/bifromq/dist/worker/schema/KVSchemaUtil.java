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

import static com.google.protobuf.ByteString.copyFromUtf8;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.FLAG_NORMAL_VAL;
import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.FLAG_ORDERED_VAL;
import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.FLAG_UNORDERED_VAL;
import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.MAX_RECEIVER_BUCKETS;
import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.SCHEMA_VER;
import static org.apache.bifromq.dist.worker.schema.KVSchemaConstants.SEPARATOR_BYTE;
import static org.apache.bifromq.dist.worker.schema.cache.RouteDetailCache.receiverBytesLen;
import static org.apache.bifromq.dist.worker.schema.cache.RouteDetailCache.tenantIdLen;
import static org.apache.bifromq.dist.worker.schema.cache.RouteGroupCache.get;
import static org.apache.bifromq.util.BSUtil.toByteString;
import static org.apache.bifromq.util.TopicConst.NUL;

import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.bifromq.dist.rpc.proto.MatchRoute;
import org.apache.bifromq.dist.rpc.proto.RouteGroup;
import org.apache.bifromq.dist.worker.schema.cache.GroupMatchingCache;
import org.apache.bifromq.dist.worker.schema.cache.Matching;
import org.apache.bifromq.dist.worker.schema.cache.NormalMatchingCache;
import org.apache.bifromq.dist.worker.schema.cache.RouteDetail;
import org.apache.bifromq.dist.worker.schema.cache.RouteDetailCache;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.util.BSUtil;

/**
 * Utility for working with the data stored in dist worker.
 */
public class KVSchemaUtil {
    public static String toReceiverUrl(MatchRoute route) {
        return toReceiverUrl(route.getBrokerId(), route.getReceiverId(), route.getDelivererKey());
    }

    public static String toReceiverUrl(int subBrokerId, String receiverId, String delivererKey) {
        return subBrokerId + NUL + receiverId + NUL + delivererKey;
    }

    public static String parseTenantId(ByteString routeKey) {
        short tenantIdLen = tenantIdLen(routeKey);
        int tenantIdStartIdx = SCHEMA_VER.size() + Short.BYTES;
        return routeKey.substring(tenantIdStartIdx, tenantIdStartIdx + tenantIdLen).toStringUtf8();
    }

    public static byte parseFlag(ByteString routeKey) {
        short receiverBytesLen = receiverBytesLen(routeKey);
        int receiverBytesStartIdx = routeKey.size() - Short.BYTES - receiverBytesLen;
        int flagByteIdx = receiverBytesStartIdx - 1;
        return routeKey.byteAt(flagByteIdx);
    }

    public static Matching buildMatchRoute(ByteString routeKey, ByteString routeValue) {
        RouteDetail routeDetail = RouteDetailCache.get(routeKey);
        if (routeDetail.matcher().getType() == RouteMatcher.Type.Normal) {
            return buildNormalMatchRoute(routeDetail, BSUtil.toLong(routeValue));
        }
        return buildGroupMatchRoute(routeDetail, get(routeValue));
    }

    public static Matching buildNormalMatchRoute(RouteDetail routeDetail, long incarnation) {
        assert routeDetail.matcher().getType() == RouteMatcher.Type.Normal;
        return NormalMatchingCache.get(routeDetail, incarnation);
    }

    public static Matching buildGroupMatchRoute(RouteDetail routeDetail, RouteGroup group) {
        assert routeDetail.matcher().getType() != RouteMatcher.Type.Normal;
        return GroupMatchingCache.get(routeDetail, group);
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
        return tenantRouteBucketStartKey(tenantId, routeMatcher.getFilterLevelList(), bucket(routeMatcher.getGroup()))
            .concat(routeMatcher.getType() == RouteMatcher.Type.OrderedShare ? FLAG_ORDERED_VAL : FLAG_UNORDERED_VAL)
            .concat(toReceiverBytes(routeMatcher.getGroup()));
    }

    private static ByteString toReceiverBytes(String receiver) {
        ByteString b = copyFromUtf8(receiver);
        return b.concat(toByteString((short) b.size()));
    }

    private static byte bucket(String receiver) {
        int hash = receiver.hashCode();
        return (byte) ((hash ^ (hash >>> 16)) & MAX_RECEIVER_BUCKETS);
    }
}
