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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.compareEndKeys;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.compareStartKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.endKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.startKey;
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.parseInboxBucketPrefix;

import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.retain.client.IRetainClient;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import org.apache.bifromq.basekv.store.api.IKVRangeSplitHinter;
import org.apache.bifromq.basekv.store.range.hinter.MutationKVLoadBasedSplitHinter;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import org.apache.bifromq.dist.client.IDistClient;
import org.apache.bifromq.inbox.client.IInboxClient;
import org.apache.bifromq.sessiondict.client.ISessionDictClient;

public class InboxStoreCoProcFactory implements IKVRangeCoProcFactory {
    private final IDistClient distClient;
    private final IInboxClient inboxClient;
    private final IRetainClient retainClient;
    private final ISessionDictClient sessionDictClient;
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final IResourceThrottler resourceThrottler;
    private final Duration detachTimeout;
    private final Duration loadEstWindow;
    private final int expireRateLimit;


    public InboxStoreCoProcFactory(IDistClient distClient,
                                   IInboxClient inboxClient,
                                   IRetainClient retainClient,
                                   ISessionDictClient sessionDictClient,
                                   ISettingProvider settingProvider,
                                   IEventCollector eventCollector,
                                   IResourceThrottler resourceThrottler,
                                   Duration detachTimeout,
                                   Duration loadEstimateWindow,
                                   int expireRateLimit) {
        this.distClient = distClient;
        this.inboxClient = inboxClient;
        this.retainClient = retainClient;
        this.sessionDictClient = sessionDictClient;
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.resourceThrottler = resourceThrottler;
        this.detachTimeout = detachTimeout;
        this.loadEstWindow = loadEstimateWindow;
        this.expireRateLimit = expireRateLimit;
    }

    @Override
    public List<IKVRangeSplitHinter> createHinters(String clusterId, String storeId, KVRangeId id,
                                                   Supplier<IKVCloseableReader> rangeReaderProvider) {
        // load-based hinter only split range around up to the inbox bucket boundary
        return Collections.singletonList(new MutationKVLoadBasedSplitHinter(loadEstWindow, key -> {
            ByteString splitKey = upperBound(parseInboxBucketPrefix(key));
            if (splitKey != null) {
                Boundary boundary = rangeReaderProvider.get().boundary();
                if (compareStartKey(startKey(boundary), splitKey) < 0
                    && compareEndKeys(splitKey, endKey(boundary)) < 0) {
                    return Optional.of(splitKey);
                }
            }
            return Optional.empty();
        },
            "clusterId", clusterId, "storeId", storeId, "rangeId",
            KVRangeIdUtil.toString(id)));
    }

    @Override
    public IKVRangeCoProc createCoProc(String clusterId,
                                       String storeId,
                                       KVRangeId id,
                                       Supplier<IKVCloseableReader> rangeReaderProvider) {
        return new InboxStoreCoProc(clusterId,
            storeId,
            id,
            distClient,
            inboxClient,
            retainClient,
            sessionDictClient,
            settingProvider,
            eventCollector,
            resourceThrottler,
            rangeReaderProvider,
            detachTimeout,
            expireRateLimit);
    }

    public void close() {
    }
}
