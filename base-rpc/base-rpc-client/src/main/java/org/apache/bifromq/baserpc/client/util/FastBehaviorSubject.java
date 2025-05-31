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

package org.apache.bifromq.baserpc.client.util;

import static java.util.Collections.emptySet;

import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.util.AppendOnlyLinkedArrayList;
import io.reactivex.rxjava3.internal.util.ExceptionHelper;
import io.reactivex.rxjava3.internal.util.NotificationLite;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A faster version of BehaviorSubject for internal use.
 */
public final class FastBehaviorSubject<T> extends Subject<T> {
    private final Set<BehaviorObserver<T>> terminated = emptySet();
    private final AtomicReference<Object> currentValue;
    private final AtomicReference<Set<BehaviorObserver<T>>> observerSet;
    private final Lock readLock;
    private final Lock writeLock;
    private final AtomicReference<Throwable> terminalThrowable;
    private long version;

    FastBehaviorSubject(T defaultVal) {
        ReadWriteLock rwLock = new ReentrantReadWriteLock();
        this.readLock = rwLock.readLock();
        this.writeLock = rwLock.writeLock();
        this.observerSet = new AtomicReference<>(ConcurrentHashMap.newKeySet());
        this.currentValue = new AtomicReference<>(defaultVal);
        this.terminalThrowable = new AtomicReference<>();
    }

    public static <T> FastBehaviorSubject<T> create() {
        return new FastBehaviorSubject<>(null);
    }

    @Override
    protected void subscribeActual(Observer<? super T> observer) {
        BehaviorObserver<T> bo = new BehaviorObserver<>(observer, this);
        observer.onSubscribe(bo);
        if (registerObserver(bo)) {
            if (bo.cancelled) {
                unregisterObserver(bo);
            } else {
                bo.emitInitial();
            }
        } else {
            Throwable ex = terminalThrowable.get();
            if (ex == ExceptionHelper.TERMINATED) {
                observer.onComplete();
            } else {
                observer.onError(ex);
            }
        }
    }

    @Override
    public void onSubscribe(Disposable d) {
        if (terminalThrowable.get() != null) {
            d.dispose();
        }
    }

    @Override
    public void onNext(T t) {
        if (terminalThrowable.get() != null) {
            return;
        }
        Object notification = NotificationLite.next(t);
        updateCurrent(notification);
        for (BehaviorObserver<T> bo : observerSet.get()) {
            bo.emitNext(notification, version);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!terminalThrowable.compareAndSet(null, t)) {
            RxJavaPlugins.onError(t);
            return;
        }
        Object errNotification = NotificationLite.error(t);
        for (BehaviorObserver<T> bo : terminateObservers(errNotification)) {
            bo.emitNext(errNotification, version);
        }
    }

    @Override
    public void onComplete() {
        if (!terminalThrowable.compareAndSet(null, ExceptionHelper.TERMINATED)) {
            return;
        }
        Object completeNotification = NotificationLite.complete();
        for (BehaviorObserver<T> bo : terminateObservers(completeNotification)) {
            bo.emitNext(completeNotification, version);
        }
    }

    @Override
    public boolean hasObservers() {
        return !observerSet.get().isEmpty();
    }

    @Override
    public Throwable getThrowable() {
        Object val = currentValue.get();
        if (NotificationLite.isError(val)) {
            return NotificationLite.getError(val);
        }
        return null;
    }

    /**
     * Get current value.
     *
     * @return value
     */
    public T getValue() {
        Object val = currentValue.get();
        if (NotificationLite.isComplete(val) || NotificationLite.isError(val)) {
            return null;
        }
        return NotificationLite.getValue(val);
    }

    @Override
    public boolean hasComplete() {
        return NotificationLite.isComplete(currentValue.get());
    }

    @Override
    public boolean hasThrowable() {
        return NotificationLite.isError(currentValue.get());
    }

    private boolean registerObserver(BehaviorObserver<T> observer) {
        Set<BehaviorObserver<T>> currentSet = observerSet.get();
        if (currentSet == terminated) {
            return false;
        }
        currentSet.add(observer);
        return true;
    }

    private void unregisterObserver(BehaviorObserver<T> observer) {
        observerSet.get().remove(observer);
    }

    private Set<BehaviorObserver<T>> terminateObservers(Object terminalNotification) {
        updateCurrent(terminalNotification);
        return observerSet.getAndSet(terminated);
    }

    private void updateCurrent(Object newValue) {
        writeLock.lock();
        try {
            version++;
            currentValue.lazySet(newValue);
        } finally {
            writeLock.unlock();
        }
    }

    private static final class BehaviorObserver<T>
        implements Disposable, AppendOnlyLinkedArrayList.NonThrowingPredicate<Object> {
        private final Observer<? super T> downstream;
        private final FastBehaviorSubject<T> parent;
        private boolean started;
        private boolean isEmitting;
        private AppendOnlyLinkedArrayList<Object> pendingQueue;
        private boolean fastPathEnabled;
        private volatile boolean cancelled;
        private long localVersion;

        BehaviorObserver(Observer<? super T> actual, FastBehaviorSubject<T> parent) {
            this.downstream = actual;
            this.parent = parent;
        }

        @Override
        public void dispose() {
            if (!cancelled) {
                cancelled = true;
                parent.unregisterObserver(this);
            }
        }

        @Override
        public boolean isDisposed() {
            return cancelled;
        }

        private void emitInitial() {
            if (cancelled) {
                return;
            }
            Object initial;
            synchronized (this) {
                if (cancelled || started) {
                    return;
                }
                FastBehaviorSubject<T> src = parent;
                Lock readLock = src.readLock;
                readLock.lock();
                try {
                    localVersion = src.version;
                    initial = src.currentValue.get();
                } finally {
                    readLock.unlock();
                }
                isEmitting = initial != null;
                started = true;
            }
            if (initial != null && !test(initial)) {
                processQueue();
            }
        }

        private void emitNext(Object notification, long newVersion) {
            if (cancelled) {
                return;
            }
            if (!fastPathEnabled) {
                synchronized (this) {
                    if (cancelled) {
                        return;
                    }
                    if (localVersion == newVersion) {
                        return;
                    }
                    if (isEmitting) {
                        if (pendingQueue == null) {
                            pendingQueue = new AppendOnlyLinkedArrayList<>(4);
                        }
                        pendingQueue.add(notification);
                        return;
                    }
                    started = true;
                }
                fastPathEnabled = true;
            }
            test(notification);
        }

        @Override
        public boolean test(Object notification) {
            return cancelled || NotificationLite.accept(notification, downstream);
        }

        private void processQueue() {
            for (; ; ) {
                if (cancelled) {
                    return;
                }
                AppendOnlyLinkedArrayList<Object> q;
                synchronized (this) {
                    q = pendingQueue;
                    if (q == null) {
                        isEmitting = false;
                        return;
                    }
                    pendingQueue = null;
                }
                q.forEachWhile(this);
            }
        }
    }
}