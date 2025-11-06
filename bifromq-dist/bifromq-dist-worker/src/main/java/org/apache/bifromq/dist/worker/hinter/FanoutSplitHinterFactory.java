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

package org.apache.bifromq.dist.worker.hinter;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.bifromq.basekv.store.range.hinter.IKVRangeSplitHinter;
import org.apache.bifromq.basekv.store.range.hinter.IKVRangeSplitHinterFactory;
import org.apache.bifromq.basekv.store.range.hinter.SplitHinterContext;

public class FanoutSplitHinterFactory implements IKVRangeSplitHinterFactory {
    private static final String CONF_SPLIT_THRESHOLD = "splitThreshold";

    @Override
    public IKVRangeSplitHinter create(SplitHinterContext ctx, Struct conf) {
        int threshold = 100000; // default
        if (conf != null && conf.getFieldsOrDefault(CONF_SPLIT_THRESHOLD, Value.getDefaultInstance()).hasNumberValue()) {
            threshold = (int) conf.getFieldsOrThrow(CONF_SPLIT_THRESHOLD).getNumberValue();
        }
        return new FanoutSplitHinter(ctx.getReaderProvider(), threshold, ctx.getTags());
    }
}

