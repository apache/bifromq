/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.baserpc.client;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.bifromq.baserpc.BluePrint;
import org.apache.bifromq.baserpc.client.loadbalancer.IServerGroupRouter;
import org.apache.bifromq.baserpc.client.loadbalancer.IServerSelector;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.schedulers.Schedulers;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class ManagedBiDiStream<InT, OutT> {

    protected final BluePrint.BalanceMode balanceMode;
    private final AtomicReference<State> state = new AtomicReference<>(State.Init);
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final String tenantId;
    private final String wchKey;
    private final String targetServerId;
    private final Supplier<Map<String, String>> metadataSupplier;
    private final Channel channel;
    private final CallOptions callOptions;
    private final MethodDescriptor<InT, OutT> methodDescriptor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<BidiStreamContext<InT, OutT>> bidiStream =
        new AtomicReference<>(BidiStreamContext.from(new DummyBiDiStream<>(this)));
    private final AtomicBoolean retargetScheduled = new AtomicBoolean();
    private volatile IServerSelector serverSelector = DummyServerSelector.INSTANCE;
    ManagedBiDiStream(String tenantId,
                      @Nullable String wchKey,
                      @Nullable String targetServerId,
                      BluePrint.BalanceMode balanceMode,
                      Supplier<Map<String, String>> metadataSupplier,
                      Channel channel,
                      CallOptions callOptions,
                      MethodDescriptor<InT, OutT> methodDescriptor) {
        checkArgument(balanceMode != BluePrint.BalanceMode.DDBalanced || targetServerId != null,
            "targetServerId is required");
        checkArgument(balanceMode != BluePrint.BalanceMode.WCHBalanced | wchKey != null, "wchKey is required");
        this.tenantId = tenantId;
        this.wchKey = wchKey;
        this.targetServerId = targetServerId;
        this.balanceMode = balanceMode;
        this.metadataSupplier = metadataSupplier;
        this.channel = channel;
        this.callOptions = callOptions;
        this.methodDescriptor = methodDescriptor;
    }

    void start(Observable<IServerSelector> serverSelectorObservable) {
        disposables.add(serverSelectorObservable
            .subscribeOn(Schedulers.io())
            .subscribe(this::onServerSelectorChanged));
    }

    State state() {
        return state.get();
    }

    final boolean isReady() {
        return bidiStream.get().bidiStream().isReady();
    }

    abstract boolean prepareRetarget();

    abstract boolean canStartRetarget();

    abstract void onStreamCreated();

    abstract void onStreamReady();

    abstract void onStreamError(Throwable e);

    abstract void onNoServerAvailable();

    abstract void onServiceUnavailable();

    private void reportNoServerAvailable() {
        log.debug("Stream@{} no server available to target: method={}",
            this.hashCode(), methodDescriptor.getBareMethodName());
        onNoServerAvailable();
    }

    private void reportServiceUnavailable() {
        log.debug("Stream@{} service unavailable to target: method={}",
            this.hashCode(), methodDescriptor.getBareMethodName());
        onServiceUnavailable();
    }

    abstract void onReceive(OutT out);

    private void onServerSelectorChanged(IServerSelector newServerSelector) {
        log.debug("Stream@{} server selector changed: method={}, balanceMode={},state={}\n{}",
            this.hashCode(), methodDescriptor.getBareMethodName(), balanceMode, state.get(), newServerSelector);
        switch (balanceMode) {
            case DDBalanced -> {
                this.serverSelector = newServerSelector;
                switch (state.get()) {
                    // target the server if it is available
                    case Init, StreamDisconnect, NoServerAvailable -> scheduleRetargetNow();
                    default -> {
                        // Normal, PendingRetarget, Retargeting do nothing
                    }
                }
            }
            case WCHBalanced -> {
                IServerGroupRouter router = newServerSelector.get(tenantId);
                IServerGroupRouter prevRouter = this.serverSelector.get(tenantId);
                // no change in server group, no need to retarget
                this.serverSelector = newServerSelector;
                if (router.isSameGroup(prevRouter)) {
                    return;
                }
                Optional<String> currentServer = prevRouter.hashing(wchKey);
                Optional<String> newServer = router.hashing(wchKey);
                if (newServer.isEmpty()) {
                    // cancel current bidi-stream
                    synchronized (this) {
                        bidiStream.get().bidiStream().cancel("no server available");
                    }
                } else if (!newServer.equals(currentServer)) {
                    switch (state.get()) {
                        // trigger graceful retarget process
                        case Normal -> gracefulRetarget();
                        // target the server if it is available
                        case Init, StreamDisconnect, NoServerAvailable -> scheduleRetargetNow();
                        default -> {
                            // PendingRetarget, Retargeting do nothing
                        }
                    }
                }
            }
            case WRBalanced -> {
                IServerGroupRouter router = newServerSelector.get(tenantId);
                IServerGroupRouter prevRouter = this.serverSelector.get(tenantId);
                // no change in server group, no need to retarget
                this.serverSelector = newServerSelector;
                if (router.isSameGroup(prevRouter)) {
                    return;
                }
                Optional<String> newServer = router.random();
                if (newServer.isEmpty()) {
                    // cancel current bidi-stream
                    synchronized (this) {
                        bidiStream.get().bidiStream().cancel("no server available");
                    }
                } else {
                    switch (state.get()) {
                        case Normal -> {
                            // compare current server and new server
                            if (newServer.get().equals(bidiStream.get().bidiStream().serverId())) {
                                return;
                            }
                            // trigger graceful retarget process if needed
                            gracefulRetarget();
                        }
                        // schedule a task to build bidi-stream to target server
                        case Init, StreamDisconnect, NoServerAvailable -> scheduleRetargetNow();
                        default -> {
                            // PendingRetarget, Retargeting do nothing
                        }
                    }
                }
            }
            default -> {
                assert balanceMode == BluePrint.BalanceMode.WRRBalanced;
                IServerGroupRouter router = newServerSelector.get(tenantId);
                IServerGroupRouter prevRouter = this.serverSelector.get(tenantId);
                // no change in server group, no need to retarget
                this.serverSelector = newServerSelector;
                if (router.isSameGroup(prevRouter)) {
                    return;
                }
                Optional<String> newServer = router.tryRoundRobin();
                if (newServer.isEmpty()) {
                    // cancel current bidi-stream
                    synchronized (this) {
                        bidiStream.get().bidiStream().cancel("no server available");
                    }
                } else {
                    switch (state.get()) {
                        // trigger graceful retarget process if needed
                        case Normal -> gracefulRetarget();
                        // schedule a task to build bidi-stream to target server
                        case Init, StreamDisconnect, NoServerAvailable -> scheduleRetargetNow();
                        default -> {
                            // PendingRetarget, Retargeting do nothing
                        }
                    }
                }
            }
        }
    }

    void send(InT in) {
        bidiStream.get().bidiStream().send(in);
    }

    void close() {
        synchronized (this) {
            disposables.dispose();
            bidiStream.get().close();
            closed.set(true);
        }
    }

    private void gracefulRetarget() {
        if (state.compareAndSet(State.Normal, State.PendingRetarget)) {
            log.debug("Stream@{} start graceful retarget process: method={}",
                this.hashCode(), methodDescriptor.getBareMethodName());
            if (prepareRetarget()) {
                // if it's ready to retarget, close it and start a new one
                log.debug("Stream@{} close current bidi-stream immediately before retargeting: method={}",
                    this.hashCode(), methodDescriptor.getBareMethodName());
                state.set(State.Retargeting);
                bidiStream.get().close();
                scheduleRetargetNow();
            }
            // TODO: set up a timer to defense against the case that onNext is not called
            //  which may happen in buggy server implementation.
        }
    }

    private void scheduleRetargetWithRandomDelay() {
        long delay = ThreadLocalRandom.current().nextLong(500, 1500);
        scheduleRetarget(Duration.ofMillis(delay));
    }

    private void scheduleRetargetNow() {
        scheduleRetarget(Duration.ZERO);
    }

    private void scheduleRetarget(Duration delay) {
        if (retargetScheduled.compareAndSet(false, true)) {
            log.debug("Stream@{} schedule retarget task in {}ms: method={}",
                this.hashCode(), delay.toMillis(), methodDescriptor.getBareMethodName());
            CompletableFuture.runAsync(() -> retarget(this.serverSelector),
                CompletableFuture.delayedExecutor(delay.toMillis(), MILLISECONDS));
        }
    }

    private void retarget(IServerSelector serverSelector) {
        synchronized (this) {
            if (closed.get()) {
                return;
            }
            switch (balanceMode) {
                case DDBalanced -> {
                    boolean available = serverSelector.exists(targetServerId);
                    if (available) {
                        target(targetServerId);
                    } else {
                        // delay MTTR(network partition) to declare no server available?
                        state.set(State.NoServerAvailable);
                        reportNoServerAvailable();
                    }
                }
                case WCHBalanced -> {
                    IServerGroupRouter router = serverSelector.get(tenantId);
                    Optional<String> selectedServer = router.hashing(wchKey);
                    if (selectedServer.isEmpty()) {
                        state.set(State.NoServerAvailable);
                        reportServiceUnavailable();
                    } else {
                        target(selectedServer.get());
                    }
                }
                case WRBalanced -> {
                    IServerGroupRouter router = serverSelector.get(tenantId);
                    Optional<String> selectedServer = router.random();
                    if (selectedServer.isEmpty()) {
                        state.set(State.NoServerAvailable);
                        reportServiceUnavailable();
                    } else {
                        target(selectedServer.get());
                    }
                }
                default -> {
                    assert balanceMode == BluePrint.BalanceMode.WRRBalanced;
                    IServerGroupRouter router = serverSelector.get(tenantId);
                    Optional<String> selectedServer = router.roundRobin();
                    if (selectedServer.isEmpty()) {
                        state.set(State.NoServerAvailable);
                        reportServiceUnavailable();
                    } else {
                        target(selectedServer.get());
                    }
                }
            }
        }
        retargetScheduled.set(false);
        if (serverSelector != this.serverSelector) {
            // server selector has been changed, schedule a retarget
            scheduleRetargetNow();
        }
    }

    private void target(String serverId) {
        if (state.compareAndSet(State.Init, State.Normal)
            || state.compareAndSet(State.StreamDisconnect, State.Normal)
            || state.compareAndSet(State.NoServerAvailable, State.Normal)
            || state.compareAndSet(State.Retargeting, State.Normal)) {
            log.debug("Stream@{} build bidi-stream to target server[{}]: method={}",
                this.hashCode(), serverId, methodDescriptor.getBareMethodName());
            BidiStreamContext<InT, OutT> bidiStreamContext = BidiStreamContext.from(new BiDiStream<>(
                tenantId,
                serverId,
                channel,
                methodDescriptor,
                metadataSupplier.get(),
                callOptions));
            bidiStream.set(bidiStreamContext);
            onStreamCreated();
            bidiStreamContext.subscribe(this::onNext, this::onError, this::onCompleted);
            bidiStreamContext.onReady(ts -> onStreamReady());
        }
        if (bidiStream.get().bidiStream().isReady()) {
            log.debug("Stream@{} ready after build to server[{}]: method={}",
                this.hashCode(), serverId, methodDescriptor.getBareMethodName());
            onStreamReady();
        }
    }

    private void onNext(OutT out) {
        onReceive(out);
        // check retarget progress
        if (state.get() == State.PendingRetarget && canStartRetarget()) {
            // do not close the stream inline
            CompletableFuture.runAsync(() -> {
                log.debug("Stream@{} close current bidi-stream before retargeting: method={}",
                    this.hashCode(), methodDescriptor.getBareMethodName());
                state.set(State.Retargeting);
                bidiStream.get().close();
                scheduleRetargetNow();
            });
        }
    }

    private void onError(Throwable t) {
        log.debug("BidiStream@{} error: method={}", this.hashCode(), methodDescriptor.getBareMethodName(), t);
        state.compareAndSet(State.Normal, State.StreamDisconnect);
        onStreamError(t);
        scheduleRetargetWithRandomDelay();
    }

    private void onCompleted() {
        log.debug("BidiStream@{} complete: method={}", this.hashCode(), methodDescriptor.getBareMethodName());
        // server gracefully close the stream
        state.compareAndSet(State.Normal, State.StreamDisconnect);
        onStreamError(new CancellationException("server close the bidi-stream"));
        scheduleRetargetWithRandomDelay();
    }

    enum State {
        Init,
        Normal,
        PendingRetarget,
        Retargeting,
        StreamDisconnect,
        NoServerAvailable
    }

    private record DummyBiDiStream<InT, OutT>(ManagedBiDiStream<InT, OutT> managedBiDiStream)
        implements IBiDiStream<InT, OutT> {

        @Override
        public Observable<OutT> onNext() {
            return Observable.empty();
        }

        @Override
        public Observable<Long> onReady() {
            return Observable.empty();
        }

        @Override
        public String serverId() {
            return "";
        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void cancel(String message) {
            // do nothing
            managedBiDiStream.onStreamError(new IllegalStateException("bidi-stream is not ready"));
        }

        @Override
        public void send(InT in) {
            // do nothing
            managedBiDiStream.onStreamError(new IllegalStateException("bidi-stream is not ready"));
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    private record BidiStreamContext<InT, OutT>(IBiDiStream<InT, OutT> bidiStream, CompositeDisposable disposable) {
        static <InT, OutT> BidiStreamContext<InT, OutT> from(IBiDiStream<InT, OutT> bidiStream) {
            return new BidiStreamContext<>(bidiStream, new CompositeDisposable());
        }

        void subscribe(Consumer<OutT> onNext, Consumer<Throwable> onError, Action onComplete) {
            disposable.add(bidiStream.onNext().subscribe(onNext, onError, onComplete));
        }

        void onReady(Consumer<Long> onReady) {
            disposable.add(bidiStream.onReady().subscribe(onReady));
        }

        void close() {
            disposable.dispose();
            bidiStream.close();
        }
    }
}
