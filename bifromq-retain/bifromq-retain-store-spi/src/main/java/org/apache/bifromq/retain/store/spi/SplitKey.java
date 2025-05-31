/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.retain.store.spi;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.filterPrefix;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.retainKeyPrefix;
import static org.apache.bifromq.retain.store.schema.KVSchemaUtil.tenantBeginKey;
import static org.apache.bifromq.util.TopicUtil.parse;
import static com.google.protobuf.UnsafeByteOperations.unsafeWrap;

import org.apache.bifromq.basekv.proto.Boundary;
import com.google.protobuf.ByteString;
import java.util.List;

/**
 * Utility for generating valid split key for retain store.
 */
public class SplitKey {
    /**
     * The key boundary of the tenant.
     *
     * @param tenantId the tenant id
     * @return the boundary
     */
    public static Boundary tenantBoundary(String tenantId) {
        ByteString tenantBeginKey = tenantBeginKey(tenantId);
        return toBoundary(tenantBeginKey, upperBound(tenantBeginKey));
    }

    /**
     * Topic pattern is similar to the topic filter, with following limit.
     * <ul>
     *     <li>Multi-level wildcard "#" is not allowed</li>
     *     <li>One and only one single-level wildcard "+" is allowed</li>
     * </ul>
     * Usually single level wildcard is used to represent the actual topic level with large cardinals.
     *
     * @param tenantId     the tenant id
     * @param topicPattern the topic pattern to represent actual retain message topics
     * @param bucket       the bucket number under the topic pattern
     * @return the boundary
     */
    public static Boundary retainBucketBoundary(String tenantId, String topicPattern, byte bucket) {
        int idx = topicPattern.indexOf("+");
        if (idx < 0 || topicPattern.indexOf("+") != topicPattern.lastIndexOf("+")) {
            throw new IllegalArgumentException("Invalid topic pattern: " + topicPattern);
        }
        List<String> topicLevels = parse(topicPattern, false);
        List<String> prefixLevels = filterPrefix(topicLevels);
        ByteString splitKey = retainKeyPrefix(tenantId, (short) topicLevels.size(), prefixLevels)
            .concat(unsafeWrap(new byte[] {bucket}));
        return toBoundary(splitKey, upperBound(splitKey));
    }
}
