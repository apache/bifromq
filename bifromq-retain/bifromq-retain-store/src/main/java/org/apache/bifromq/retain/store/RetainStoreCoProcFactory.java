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

package org.apache.bifromq.retain.store;

import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProcFactory;
import org.apache.bifromq.basekv.store.api.IKVRangeSplitHinter;
import org.apache.bifromq.basekv.store.range.hinter.MutationKVLoadBasedSplitHinter;
import org.apache.bifromq.basekv.utils.KVRangeIdUtil;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class RetainStoreCoProcFactory implements IKVRangeCoProcFactory {
    private final Duration loadEstWindow;

    public RetainStoreCoProcFactory(Duration loadEstimateWindow) {
        this.loadEstWindow = loadEstimateWindow;
    }

    @Override
    public List<IKVRangeSplitHinter> createHinters(String clusterId, String storeId, KVRangeId id,
                                                   Supplier<IKVCloseableReader> rangeReaderProvider) {
        return Collections.singletonList(
            new MutationKVLoadBasedSplitHinter(loadEstWindow, Optional::of,
                "clusterId", clusterId, "storeId", storeId, "rangeId", KVRangeIdUtil.toString(id)));
    }

    @Override
    public IKVRangeCoProc createCoProc(String clusterId,
                                       String storeId,
                                       KVRangeId id,
                                       Supplier<IKVCloseableReader> rangeReaderProvider) {
        return new RetainStoreCoProc(clusterId, storeId, id, rangeReaderProvider);
    }

    public void close() {
    }
}
