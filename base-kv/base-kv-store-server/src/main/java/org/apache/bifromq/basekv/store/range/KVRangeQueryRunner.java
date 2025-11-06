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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.bifromq.basekv.proto.State.StateType.Merged;
import static org.apache.bifromq.basekv.proto.State.StateType.Removed;
import static org.apache.bifromq.basekv.proto.State.StateType.ToBePurged;
import static org.apache.bifromq.basekv.store.util.VerUtil.boundaryCompatible;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;
import org.apache.bifromq.basekv.proto.KVRangeDescriptor;
import org.apache.bifromq.basekv.proto.State;
import org.apache.bifromq.basekv.store.api.IKVRangeCoProc;
import org.apache.bifromq.basekv.store.api.IKVRangeRefreshableReader;
import org.apache.bifromq.basekv.store.exception.KVRangeException;
import org.apache.bifromq.basekv.store.proto.ROCoProcInput;
import org.apache.bifromq.basekv.store.proto.ROCoProcOutput;
import org.apache.bifromq.basekv.store.range.hinter.IKVLoadRecord;
import org.apache.bifromq.basekv.store.range.hinter.IKVRangeSplitHinter;
import org.apache.bifromq.basekv.utils.BoundaryUtil;
import org.apache.bifromq.logger.MDCLogger;
import org.slf4j.Logger;

class KVRangeQueryRunner implements IKVRangeQueryRunner {
    private final Logger log;
    private final IKVRange kvRange;
    private final IKVRangeCoProc coProc;
    private final Executor executor;
    private final Set<CompletableFuture<?>> runningQueries = Sets.newConcurrentHashSet();
    private final IKVRangeQueryLinearizer linearizer;
    private final StampedLock queryLock;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<IKVRangeSplitHinter> splitHinters;
    private final Supplier<KVRangeDescriptor> descriptorSupplier;

    KVRangeQueryRunner(IKVRange kvRange,
                       IKVRangeCoProc coProc,
                       Executor executor,
                       IKVRangeQueryLinearizer linearizer,
                       List<IKVRangeSplitHinter> splitHinters,
                       Supplier<KVRangeDescriptor> descriptorSupplier,
                       StampedLock queryLock,
                       String... tags) {
        this.kvRange = kvRange;
        this.coProc = coProc;
        this.executor = executor;
        this.linearizer = linearizer;
        this.queryLock = queryLock;
        this.splitHinters = splitHinters;
        this.descriptorSupplier = descriptorSupplier;
        this.log = MDCLogger.getLogger(KVRangeQueryRunner.class, tags);
    }

    // Execute a ROCommand
    @Override
    public CompletableFuture<Boolean> exist(long ver, ByteString key, boolean linearized) {
        return submit(ver, rangeReader -> {
            Preconditions.checkArgument(BoundaryUtil.inRange(key, rangeReader.boundary()));
            return completedFuture(rangeReader.exist(key));
        }, linearized);
    }

    @Override
    public CompletableFuture<Optional<ByteString>> get(long ver, ByteString key, boolean linearized) {
        return submit(ver, rangeReader -> {
            Preconditions.checkArgument(BoundaryUtil.inRange(key, rangeReader.boundary()));
            return completedFuture(rangeReader.get(key));
        }, linearized);
    }

    @Override
    public CompletableFuture<ROCoProcOutput> queryCoProc(long ver, ROCoProcInput query, boolean linearized) {
        return submit(ver, rangeReader -> {
            IKVLoadRecorder loadRecorder = new KVLoadRecorder();
            IKVRangeRefreshableReader loadRecordableReader = new LoadRecordableKVReader(rangeReader, loadRecorder);
            return coProc.query(query, loadRecordableReader)
                .whenComplete((v, e) -> {
                    try {
                        IKVLoadRecord record = loadRecorder.stop();
                        splitHinters.forEach(hinter -> hinter.recordQuery(query, record));
                    } catch (Throwable t) {
                        log.error("Failed to reset hinter and coProc", t);
                    }
                });
        }, linearized);
    }

    // Close the executor, the returned future will be completed when running commands finished and pending tasks
    // will be canceled
    public void close() {
        if (closed.compareAndSet(false, true)) {
            runningQueries.forEach(f -> f.cancel(true));
        }
    }

    private <ReqT, ResultT> CompletableFuture<ResultT> submit(long ver,
                                                              QueryFunction<ReqT, ResultT> queryFn,
                                                              boolean linearized) {
        if (!boundaryCompatible(ver, kvRange.currentVer())) {
            return CompletableFuture.failedFuture(
                new KVRangeException.BadVersion("Version Mismatch", descriptorSupplier.get()));
        }
        CompletableFuture<ResultT> onDone = new CompletableFuture<>();
        runningQueries.add(onDone);
        Runnable queryTask = () -> {
            if (onDone.isDone()) {
                return;
            }
            if (closed.get()) {
                onDone.cancel(true);
                return;
            }
            if (linearized) {
                linearizer.linearize()
                    .thenComposeAsync(v -> doQuery(ver, queryFn), executor)
                    .whenCompleteAsync((r, e) -> {
                        if (e != null) {
                            onDone.completeExceptionally(e);
                        } else {
                            onDone.complete(r);
                        }
                    }, executor);
            } else {
                doQuery(ver, queryFn).whenCompleteAsync((v, e) -> {
                    if (e != null) {
                        onDone.completeExceptionally(e);
                    } else {
                        onDone.complete(v);
                    }
                }, executor);
            }
        };
        onDone.whenComplete((v, e) -> runningQueries.remove(onDone));
        executor.execute(queryTask);
        return onDone;
    }

    private <ReqT, ResultT> CompletableFuture<ResultT> doQuery(long ver, QueryFunction<ReqT, ResultT> queryFn) {
        CompletableFuture<ResultT> onDone = new CompletableFuture<>();
        long stamp = queryLock.tryReadLock();
        try {
            if (stamp == 0L) {
                // range is being reset or managed by a writer, don't block; hint client to retry with latest
                return CompletableFuture.failedFuture(
                    new KVRangeException.TryLater("Range is resetting or busy", descriptorSupplier.get()));
            }
            IKVRangeRefreshableReader refreshableReader = kvRange.newReader();
            // return the borrowed reader when future completed
            onDone.whenComplete((v, e) -> refreshableReader.close());
            State state = kvRange.currentState();
            if (!boundaryCompatible(ver, kvRange.currentVer())) {
                queryLock.unlockRead(stamp);
                onDone.completeExceptionally(
                    new KVRangeException.BadVersion("Version Mismatch", descriptorSupplier.get()));
                return onDone;
            }
            if (state.getType() == Merged || state.getType() == Removed || state.getType() == ToBePurged) {
                queryLock.unlockRead(stamp);
                onDone.completeExceptionally(
                    new KVRangeException.TryLater("Range has been in state: " + state.getType().name().toLowerCase()));
                return onDone;
            }
            refreshableReader.refresh();
            return queryFn.apply(refreshableReader)
                .whenCompleteAsync((v, e) -> {
                    queryLock.unlockRead(stamp);
                    if (e != null) {
                        onDone.completeExceptionally(e);
                    } else {
                        onDone.complete(v);
                    }
                }, executor);
        } catch (Throwable e) {
            if (stamp != 0L) {
                queryLock.unlockRead(stamp);
            }
            log.debug("Failed to query range", e);
            return CompletableFuture.failedFuture(new KVRangeException.InternalException(e.getMessage()));
        }
    }

    private interface QueryFunction<Req, Resp> {
        CompletableFuture<Resp> apply(IKVRangeRefreshableReader rangeReader);
    }
}
