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

package org.apache.bifromq.basekv.store.range.hinter;

import java.time.Duration;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;

public class QueryKVLoadBasedSplitHinter extends KVLoadBasedSplitHinter {
    public static final String TYPE = "kv_io_query";

    public QueryKVLoadBasedSplitHinter(Duration windowSize, String... tags) {
        this(System::nanoTime, windowSize, tags);
    }

    public QueryKVLoadBasedSplitHinter(Supplier<Long> nanoSource, Duration windowSize, String... tags) {
        super(nanoSource, windowSize, tags);
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
