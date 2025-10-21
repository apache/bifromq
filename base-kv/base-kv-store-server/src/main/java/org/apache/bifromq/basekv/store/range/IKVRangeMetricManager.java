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

package org.apache.bifromq.basekv.store.range;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Timer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.proto.RWCoProcOutput;

interface IKVRangeMetricManager {
    void report(KVRangeDescriptor descriptor);

    void reportDump(int bytes);

    void reportRestore(int bytes);

    void reportLastAppliedIndex(long index);

    <T> CompletableFuture<T> recordDuration(Supplier<CompletableFuture<T>> supplier, Timer timer);

    CompletableFuture<Void> recordConfigChange(Supplier<CompletableFuture<Void>> supplier);

    CompletableFuture<Void> recordTransferLeader(Supplier<CompletableFuture<Void>> supplier);

    CompletableFuture<Void> recordSplit(Supplier<CompletableFuture<Void>> supplier);

    CompletableFuture<Void> recordMerge(Supplier<CompletableFuture<Void>> supplier);

    CompletableFuture<ByteString> recordPut(Supplier<CompletableFuture<ByteString>> supplier);

    CompletableFuture<ByteString> recordDelete(Supplier<CompletableFuture<ByteString>> supplier);

    CompletableFuture<RWCoProcOutput> recordMutateCoProc(Supplier<CompletableFuture<RWCoProcOutput>> supplier);

    CompletableFuture<Boolean> recordExist(Supplier<CompletableFuture<Boolean>> supplier);

    CompletableFuture<Optional<ByteString>> recordGet(Supplier<CompletableFuture<Optional<ByteString>>> supplier);

    CompletableFuture<ROCoProcOutput> recordQueryCoProc(Supplier<CompletableFuture<ROCoProcOutput>> supplier);

    CompletableFuture<Void> recordLinearization(Supplier<CompletableFuture<Void>> supplier);

    CompletableFuture<Void> recordCompact(Supplier<CompletableFuture<Void>> supplier);

    CompletableFuture<Void> recordLogApply(Supplier<CompletableFuture<Void>> supplier);

    CompletableFuture<Void> recordSnapshotInstall(Supplier<CompletableFuture<Void>> supplier);
}
