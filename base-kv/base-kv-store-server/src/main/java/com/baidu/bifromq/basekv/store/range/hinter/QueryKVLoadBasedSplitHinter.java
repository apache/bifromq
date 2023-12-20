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

package com.baidu.bifromq.basekv.store.range.hinter;

import com.baidu.bifromq.basekv.store.api.IKVLoadRecord;
import com.baidu.bifromq.basekv.store.proto.ROCoProcInput;
import com.baidu.bifromq.basekv.store.proto.RWCoProcInput;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class QueryKVLoadBasedSplitHinter extends KVLoadBasedSplitHinter {
    public static final String TYPE = "kv_io_query";

    public QueryKVLoadBasedSplitHinter(Duration windowSize, String... tags) {
        this(windowSize, Optional::of, tags);
    }

    public QueryKVLoadBasedSplitHinter(Duration windowSize,
                                       Function<ByteString, Optional<ByteString>> toSplitKey,
                                       String... tags) {
        this(System::nanoTime, windowSize, toSplitKey, tags);
    }

    public QueryKVLoadBasedSplitHinter(Supplier<Long> nanoSource, Duration windowSize,
                                       Function<ByteString, Optional<ByteString>> toSplitKey,
                                       String... tags) {
        super(nanoSource, windowSize, toSplitKey, tags);
    }

    @Override
    public void recordQuery(ROCoProcInput input, IKVLoadRecord ioRecord) {
        onRecord(ioRecord);
    }

    @Override
    public void recordMutate(RWCoProcInput input, IKVLoadRecord ioRecord) {
        // do nothing
    }

    @Override
    protected String type() {
        return TYPE;
    }
}
