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

package com.baidu.bifromq.inbox.store;

import static com.baidu.bifromq.basekv.localengine.RangeUtil.upperBound;
import static com.baidu.bifromq.inbox.util.KeyUtil.parseScopedInboxId;

import com.baidu.bifromq.basekv.proto.KVRangeId;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProc;
import com.baidu.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import com.baidu.bifromq.basekv.store.api.IKVRangeReader;
import com.baidu.bifromq.basekv.store.range.ILoadTracker;
import com.baidu.bifromq.plugin.eventcollector.IEventCollector;
import com.baidu.bifromq.plugin.settingprovider.ISettingProvider;
import com.google.protobuf.ByteString;
import java.time.Clock;
import java.time.Duration;
import java.util.function.Supplier;

public class InboxStoreCoProcFactory implements IKVRangeCoProcFactory {
    private final ISettingProvider settingProvider;
    private final IEventCollector eventCollector;
    private final Clock clock;
    private final Duration purgeDelay;

    public InboxStoreCoProcFactory(ISettingProvider settingProvider, IEventCollector eventCollector, Clock clock,
                                   Duration purgeDelay) {
        this.settingProvider = settingProvider;
        this.eventCollector = eventCollector;
        this.clock = clock;
        this.purgeDelay = purgeDelay;
    }

    @Override
    public ByteString toSplitKey(ByteString key) {
        return upperBound(parseScopedInboxId(key));
    }

    @Override
    public IKVRangeCoProc create(KVRangeId id, Supplier<IKVRangeReader> rangeReaderProvider, ILoadTracker loadTracker) {
        return new InboxStoreCoProc(id, rangeReaderProvider, settingProvider, eventCollector, clock, purgeDelay,
            loadTracker);
    }

    public void close() {
    }
}
