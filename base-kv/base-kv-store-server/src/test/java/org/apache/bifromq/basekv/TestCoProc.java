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

package org.apache.bifromq.basekv;

import org.apache.bifromq.basekv.proto.KVRangeId;
import org.apache.bifromq.basekv.store.api.IKVCloseableReader;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.basekv.store.api.IKVWriter;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcInput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class TestCoProc implements IKVRangeCoProc {
    private final Supplier<IKVCloseableReader> rangeReaderProvider;

    public TestCoProc(KVRangeId id, Supplier<IKVCloseableReader> rangeReaderProvider) {
        this.rangeReaderProvider = rangeReaderProvider;
    }

    @Override
    public CompletableFuture<ROCoProcOutput> query(ROCoProcInput input, IKVReader reader) {
        // get
        return CompletableFuture.completedFuture(
            ROCoProcOutput.newBuilder().setRaw(reader.get(input.getRaw()).orElse(ByteString.EMPTY)).build());
    }

    @Override
    public Supplier<MutationResult> mutate(RWCoProcInput input, IKVReader reader, IKVWriter client) {
        String[] str = input.getRaw().toStringUtf8().split("_");
        ByteString key = ByteString.copyFromUtf8(str[0]);
        ByteString value = ByteString.copyFromUtf8(str[1]);
        // update
        Optional<ByteString> existing = reader.get(key);
        client.put(key, value);
        return () -> new MutationResult(RWCoProcOutput.newBuilder().setRaw(existing.orElse(ByteString.EMPTY)).build(),
            Optional.empty());
    }

    @Override
    public void close() {

    }
}
