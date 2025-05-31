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

package org.apache.bifromq.plugin.subbroker;

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class MonitoredSubBroker implements ISubBroker {
    private final AtomicBoolean hasStopped = new AtomicBoolean();
    private final ISubBroker delegate;
    private final Timer checkSubCallTimer;
    private final Timer deliverCallTimer;

    MonitoredSubBroker(ISubBroker delegate) {
        this.delegate = delegate;
        checkSubCallTimer = Timer.builder("ib.call.time")
            .tag("type", delegate.getClass().getName())
            .tag("call", "checkSub")
            .register(Metrics.globalRegistry);
        deliverCallTimer = Timer.builder("ib.call.time")
            .tag("type", delegate.getClass().getName())
            .tag("call", "deliver")
            .register(Metrics.globalRegistry);
    }

    @Override
    public int id() {
        return delegate.id();
    }

    @Override
    public CompletableFuture<CheckReply> check(CheckRequest request) {
        try {
            Timer.Sample start = Timer.start();
            return delegate.check(request)
                .whenComplete((v, e) -> start.stop(checkSubCallTimer));
        } catch (Throwable e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public IDeliverer open(String delivererKey) {
        Preconditions.checkState(!hasStopped.get());
        return new MonitoredDeliverer(delivererKey);
    }

    @Override
    public void close() {
        if (hasStopped.compareAndSet(false, true)) {
            delegate.close();
            Metrics.globalRegistry.remove(checkSubCallTimer);
            Metrics.globalRegistry.remove(deliverCallTimer);
        }
    }

    private class MonitoredDeliverer implements IDeliverer {
        private final IDeliverer deliverer;

        MonitoredDeliverer(String delivererKey) {
            deliverer = delegate.open(delivererKey);
        }

        @Override
        public CompletableFuture<DeliveryReply> deliver(DeliveryRequest request) {
            try {
                Timer.Sample start = Timer.start();
                return deliverer.deliver(request).whenComplete((v, e) -> start.stop(deliverCallTimer));
            } catch (Throwable e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public void close() {
            deliverer.close();
        }
    }
}
