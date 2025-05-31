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

package org.apache.bifromq.basecrdt.core.internal;

import org.apache.bifromq.basecrdt.ReplicaLogger;
import org.apache.bifromq.basecrdt.core.api.CausalCRDTType;
import org.apache.bifromq.basecrdt.core.api.ICRDTOperation;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDT;
import org.apache.bifromq.basecrdt.core.api.ICausalCRDTInflater;
import org.apache.bifromq.basecrdt.core.exception.CRDTCloseException;
import org.apache.bifromq.basecrdt.proto.Replacement;
import org.apache.bifromq.basecrdt.proto.Replica;
import org.apache.bifromq.basecrdt.proto.StateLattice;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;

abstract class CausalCRDTInflater<D extends IDotStore, O extends ICRDTOperation, C extends ICausalCRDT<O>>
    implements ICausalCRDTInflater<O, C> {
    private final Logger log;
    private final AtomicBoolean inflationScheduled = new AtomicBoolean(false);
    private final AtomicBoolean compactionScheduled = new AtomicBoolean(false);
    private final AtomicBoolean taskExecuting = new AtomicBoolean(false);
    private final AtomicReference<CompletableFuture<Void>> stopSignal = new AtomicReference<>();
    private final Replica replica;
    private final IReplicaStateLattice replicaStateLattice;
    private final ScheduledExecutorService executor;
    private final Duration inflationInterval;
    private final D dotStore;
    private final C crdt;
    private final ConcurrentLinkedQueue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
    private final MetricManager metricManager;
    private volatile ScheduledFuture<?> compactionTask;

    private volatile OperationExecTask currentOp = null;
    private volatile JoinTask currentJoin = null;

    CausalCRDTInflater(Replica replica, IReplicaStateLattice stateLattice,
                       ScheduledExecutorService executor,
                       Duration inflationInterval, String... tags) {
        this.replica = replica;
        this.log = new ReplicaLogger(replica, CausalCRDTInflater.class);
        this.replicaStateLattice = stateLattice;
        this.executor = executor;
        this.inflationInterval = inflationInterval;
        this.metricManager = new MetricManager(Tags.of(tags)
            .and("replica.uri", replica.getUri())
            .and("replica.id", Base64.getEncoder().encodeToString(replica.getId().toByteArray())));
        try {
            dotStore = dotStoreType().getDeclaredConstructor().newInstance();
            crdt = newCRDT(replica, dotStore, this::execute);
        } catch (InstantiationException
                 | IllegalAccessException
                 | InvocationTargetException
                 | NoSuchMethodException e) {
            throw new IllegalStateException("Unknown dot store implementation", e);
        }
        // build aggregated view of existing events
        stateLattice.lattices().forEachRemaining(causalState -> ((DotStore) dotStore).add(causalState));
    }


    @Override
    public abstract CausalCRDTType type();

    @Override
    public final C getCRDT() {
        return crdt;
    }

    @Override
    public final Replica id() {
        return replica;
    }

    @Override
    public final CompletableFuture<Void> stop() {
        CompletableFuture<Void> onStop = stopSignal.updateAndGet(current -> {
            if (current == null) {
                return new CompletableFuture<>();
            }
            return current;
        });
        scheduleInflation();
        metricManager.close();
        return onStop;
    }

    @Override
    public final CompletableFuture<Void> join(Iterable<Replacement> delta) {
        if (stopSignal.get() != null) {
            // silently drop the request
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> ret;
        synchronized (this) {
            if (currentJoin == null) {
                currentJoin = new JoinTask(delta, new CompletableFuture<>());
            } else {
                currentJoin.add(delta);
            }
            ret = currentJoin.onDone;
        }
        scheduleInflation();
        return ret;
    }

    @Override
    public final CompletableFuture<Optional<Iterable<Replacement>>> delta(
        Map<ByteString, NavigableMap<Long, Long>> coveredLatticeEvents,
        Map<ByteString, NavigableMap<Long, Long>> coveredHistoryEvents,
        int maxEvents) {
        CompletableFuture<Optional<Iterable<Replacement>>> onDone = new CompletableFuture<>();
        submitTask(() -> {
            Timer.Sample sample = Timer.start();
            onDone.complete(replicaStateLattice.delta(coveredLatticeEvents, coveredHistoryEvents, maxEvents));
            sample.stop(metricManager.deltaTimer);
        });
        return onDone;
    }

    @Override
    public final Map<ByteString, NavigableMap<Long, Long>> latticeEvents() {
        return replicaStateLattice.latticeIndex();
    }

    @Override
    public final Map<ByteString, NavigableMap<Long, Long>> historyEvents() {
        return replicaStateLattice.historyIndex();
    }

    abstract C newCRDT(Replica replica, D dotStore, CausalCRDT.CRDTOperationExecutor<O> executor);

    abstract ICoalesceOperation<D, O> startCoalescing(O op);

    abstract Class<? extends D> dotStoreType();

    private CompletableFuture<Void> execute(O op) {
        if (stopSignal.get() != null) {
            // silently drop the request
            return CompletableFuture.failedFuture(new CRDTCloseException());
        }
        CompletableFuture<Void> ret;
        synchronized (this) {
            if (currentOp == null) {
                currentOp = new OperationExecTask(op, new CompletableFuture<>());
            } else {
                currentOp.coalesce(op);
            }
            ret = currentOp.onDone;
        }
        scheduleInflation();
        return ret;
    }

    private void submitTask(Runnable task) {
        taskQueue.add(task);
        startExecutor();
    }

    private void startExecutor() {
        if (taskExecuting.compareAndSet(false, true)) {
            executor.execute(() -> {
                try {
                    while (!taskQueue.isEmpty()) {
                        Runnable task = taskQueue.poll();
                        task.run();
                        Thread.yield();
                    }
                } catch (Throwable e) {
                    log.error("Failed to execute inflater[{}] task", replica, e);
                }
                taskExecuting.set(false);
                if (!taskQueue.isEmpty()) {
                    startExecutor();
                }
            });
        }
    }

    private void scheduleInflation() {
        if (inflationScheduled.compareAndSet(false, true)) {
            Runnable task = () -> submitTask(() -> {
                try {
                    inflate();
                    scheduleCompaction();
                } catch (Throwable e) {
                    log.error("Inflation[{}] error", replica, e);
                } finally {
                    inflationScheduled.set(false);
                }
                if (currentOp != null || currentJoin != null) {
                    scheduleInflation();
                } else if (stopSignal.get() != null) {
                    if (compactionTask != null) {
                        compactionTask.cancel(true);
                    }
                    stopSignal.get().complete(null);
                }
            });
            executor.schedule(task, inflationInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void scheduleCompaction() {
        if (compactionScheduled.compareAndSet(false, true)) {
            compactionTask = executor.schedule(() -> submitTask(() -> {
                Timer.Sample sample = Timer.start();
                if (replicaStateLattice.compact() && stopSignal.get() == null) {
                    compactionScheduled.set(false);
                    scheduleCompaction();
                } else {
                    compactionScheduled.set(false);
                }
                sample.stop(metricManager.compactionTimer);
            }), replicaStateLattice.historyDuration().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void inflate() {
        Timer.Sample sample = Timer.start();
        List<Replacement> deltas = null;
        Optional<JoinTask> opTask = buildOpTask();
        if (opTask.isPresent()) {
            deltas = opTask.get().deltas;
        }
        JoinTask joinTask;
        synchronized (this) {
            joinTask = currentJoin;
            currentJoin = null;
        }
        if (joinTask != null) {
            if (deltas == null) {
                deltas = joinTask.deltas;
            } else {
                deltas.addAll(joinTask.deltas);
            }
        }

        if (deltas != null) {
            // join delta first
            IReplicaStateLattice.JoinDiff joinDiff = replicaStateLattice.join(deltas);

            // inflate the dotStore
            List<StateLattice> adds = Lists.newArrayList();
            List<StateLattice> rems = Lists.newArrayList();
            for (StateLattice lattice : joinDiff.adds()) {
                if (((DotStore) dotStore).add(lattice)) {
                    adds.add(lattice);
                }
            }
            for (StateLattice lattice : joinDiff.removes()) {
                if (((DotStore) dotStore).remove(lattice)) {
                    rems.add(lattice);
                }
            }
            if (!adds.isEmpty() || !rems.isEmpty()) {
                // tell CRDT about what happened in the dot store
                ((CausalCRDT<D, O>) crdt).afterInflation(adds, rems);
            }
        }
        sample.stop(metricManager.inflationTimer);
        if (opTask.isPresent()) {
            opTask.get().onDone.complete(null);
        }
        if (joinTask != null) {
            joinTask.onDone.complete(null);
        }
    }

    private Optional<JoinTask> buildOpTask() {
        OperationExecTask task;
        synchronized (this) {
            task = currentOp;
            currentOp = null;
        }
        if (task != null) {
            JoinTask joinTask = new JoinTask(task.op.delta(dotStore, replicaStateLattice::nextEvent),
                new CompletableFuture<>());
            joinTask.onDone.whenComplete((v, e) -> task.onDone.complete(null));
            return Optional.of(joinTask);
        }
        return Optional.empty();
    }

    private static class JoinTask {
        public final List<Replacement> deltas = Lists.newLinkedList();
        public final CompletableFuture<Void> onDone;

        JoinTask(Iterable<Replacement> delta, CompletableFuture<Void> onDone) {
            delta.forEach(deltas::add);
            this.onDone = onDone;
        }

        public void add(Iterable<Replacement> next) {
            next.forEach(deltas::add);
        }
    }

    private class OperationExecTask {
        private final CompletableFuture<Void> onDone;
        private final ICoalesceOperation<D, O> op;

        OperationExecTask(O op, CompletableFuture<Void> onDone) {
            this.op = startCoalescing(op);
            this.onDone = onDone;
        }

        public void coalesce(O next) {
            op.coalesce(next);
        }
    }

    private class MetricManager {
        public final Timer inflationTimer;
        public final Timer deltaTimer;
        public final Timer compactionTimer;
        public final Gauge eventSizeGauge;

        MetricManager(Tags tags) {
            inflationTimer = Metrics.timer("basecrdt.inflation.time", tags);
            deltaTimer = Metrics.timer("basecrdt.delta.time", tags);
            compactionTimer = Metrics.timer("basecrdt.compact.time", tags);
            eventSizeGauge = Gauge.builder("basecrdt.event.size", replicaStateLattice::size)
                .tags(tags)
                .register(Metrics.globalRegistry);
        }

        void close() {
            Metrics.globalRegistry.remove(inflationTimer.getId());
            Metrics.globalRegistry.remove(deltaTimer.getId());
            Metrics.globalRegistry.remove(compactionTimer.getId());
            Metrics.globalRegistry.remove(eventSizeGauge);
        }
    }
}
