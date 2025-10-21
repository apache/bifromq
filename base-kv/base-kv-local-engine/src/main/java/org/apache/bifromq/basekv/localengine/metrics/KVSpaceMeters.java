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

package org.apache.bifromq.basekv.localengine.metrics;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import java.lang.ref.Cleaner;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

public class KVSpaceMeters {
    private static final Cleaner CLEANER = Cleaner.create();
    private static final Cache<MeterKey, Meter> METERS = Caffeine.newBuilder().weakValues().build();

    public static Timer getTimer(String id, IKVSpaceMetric metric, Tags tags) {
        assert metric.meterType() == Meter.Type.TIMER;
        return (Timer) METERS.get(new MeterKey(id, metric, tags),
            k -> new TimerWrapper(Timer.builder(metric.metricName())
                .tags(tags)
                .tags("kvspace", id)
                .register(Metrics.globalRegistry)));
    }

    public static Counter getCounter(String id, IKVSpaceMetric metric, Tags tags) {
        assert metric.meterType() == Meter.Type.COUNTER;
        return (Counter) METERS.get(new MeterKey(id, metric, tags),
            k -> new CounterWrapper(Counter.builder(metric.metricName())
                .tags(tags)
                .tags("kvspace", id)
                .register(Metrics.globalRegistry)));
    }

    public static <C> FunctionCounter getFunctionCounter(String id,
                                                         IKVSpaceMetric metric,
                                                         C ctr,
                                                         ToDoubleFunction<C> supplier,
                                                         Tags tags) {
        assert metric.meterType() == Meter.Type.COUNTER && metric.isFunction();
        return (FunctionCounter) METERS.get(new MeterKey(id, metric, tags),
            k -> new FunctionCounterWrapper(FunctionCounter.builder(metric.metricName(), ctr, supplier)
                .tags(tags)
                .tags("kvspace", id)
                .register(Metrics.globalRegistry)));
    }

    public static <C> FunctionTimer getFunctionTimer(String id,
                                                       IKVSpaceMetric metric,
                                                       C timedObj,
                                                       ToLongFunction<C> countFunction,
                                                       ToDoubleFunction<C> totalTimeFunction,
                                                       TimeUnit timeUnit,
                                                       Tags tags) {
        assert metric.meterType() == Meter.Type.TIMER && metric.isFunction();
        return (FunctionTimer) METERS.get(new MeterKey(id, metric, tags),
            k -> new FunctionTimerWrapper(
                FunctionTimer.builder(metric.metricName(), timedObj, countFunction, totalTimeFunction, timeUnit)
                    .tags(tags)
                    .tags("kvspace", id)
                    .register(Metrics.globalRegistry)));
    }

    public static Gauge getGauge(String id, IKVSpaceMetric metric, Supplier<Number> numProvider, Tags tags) {
        assert metric.meterType() == Meter.Type.GAUGE;
        return (Gauge) METERS.get(new MeterKey(id, metric, tags),
            k -> new GaugeWrapper(Gauge.builder(metric.metricName(), numProvider)
                .tags(tags)
                .tags("kvspace", id)
                .register(Metrics.globalRegistry)));
    }

    public static DistributionSummary getSummary(String id, IKVSpaceMetric metric, Tags tags) {
        assert metric.meterType() == Meter.Type.DISTRIBUTION_SUMMARY;
        return (DistributionSummary) METERS.get(new MeterKey(id, metric, tags),
            k -> new SummaryWrapper(DistributionSummary.builder(metric.metricName())
                .tags(tags)
                .tags("kvspace", id)
                .register(Metrics.globalRegistry)));
    }

    private record MeterKey(String id, IKVSpaceMetric metric, Tags tags) {

    }

    private record State(Meter meter) implements Runnable {
        @Override
        public void run() {
            Metrics.globalRegistry.remove(meter);
        }
    }

    private static final class TimerWrapper implements Timer {
        private final Timer delegate;
        private final Cleaner.Cleanable cleanable;

        private TimerWrapper(Timer delegate) {
            this.delegate = delegate;
            cleanable = CLEANER.register(this, new State(delegate));
        }

        @Override
        public void record(long amount, TimeUnit unit) {
            delegate.record(amount, unit);
        }

        @Override
        public <T> T record(Supplier<T> f) {
            return delegate.record(f);
        }

        @Override
        public <T> T recordCallable(Callable<T> f) throws Exception {
            return delegate.recordCallable(f);
        }

        @Override
        public void record(Runnable f) {
            delegate.record(f);
        }

        @Override
        public long count() {
            return delegate.count();
        }

        @Override
        public double totalTime(TimeUnit unit) {
            return delegate.totalTime(unit);
        }

        @Override
        public double max(TimeUnit unit) {
            return delegate.max(unit);
        }

        @Override
        public TimeUnit baseTimeUnit() {
            return delegate.baseTimeUnit();
        }

        @Override
        public HistogramSnapshot takeSnapshot() {
            return delegate.takeSnapshot();
        }

        @Override
        public Id getId() {
            return delegate.getId();
        }

        @Override
        public void close() {
            delegate.close();
            cleanable.clean();
        }
    }

    private static final class FunctionTimerWrapper implements FunctionTimer {
        private final FunctionTimer delegate;
        private final Cleaner.Cleanable cleanable;

        private FunctionTimerWrapper(FunctionTimer delegate) {
            this.delegate = delegate;
            cleanable = CLEANER.register(this, new State(delegate));
        }

        @Override
        public Id getId() {
            return delegate.getId();
        }

        @Override
        public void close() {
            delegate.close();
            cleanable.clean();
        }

        @Override
        public double count() {
            return delegate.count();
        }

        @Override
        public double totalTime(TimeUnit unit) {
            return delegate.totalTime(unit);
        }

        @Override
        public TimeUnit baseTimeUnit() {
            return delegate.baseTimeUnit();
        }
    }

    private static final class FunctionCounterWrapper implements FunctionCounter {
        private final FunctionCounter delegate;
        private final Cleaner.Cleanable cleanable;

        private FunctionCounterWrapper(FunctionCounter delegate) {
            this.delegate = delegate;
            cleanable = CLEANER.register(this, new State(delegate));
        }

        @Override
        public double count() {
            return delegate.count();
        }

        @Override
        public Id getId() {
            return delegate.getId();
        }

        @Override
        public void close() {
            delegate.close();
            cleanable.clean();
        }
    }

    private static final class CounterWrapper implements Counter {
        private final Counter delegate;
        private final Cleaner.Cleanable cleanable;

        private CounterWrapper(Counter delegate) {
            this.delegate = delegate;
            cleanable = CLEANER.register(this, new State(delegate));
        }

        @Override
        public Id getId() {
            return delegate.getId();
        }

        @Override
        public void close() {
            delegate.close();
            cleanable.clean();
        }

        @Override
        public void increment(double amount) {
            delegate.increment(amount);
        }

        @Override
        public double count() {
            return delegate.count();
        }
    }

    private static final class GaugeWrapper implements Gauge {
        private final Gauge delegate;
        private final Cleaner.Cleanable cleanable;

        private GaugeWrapper(Gauge delegate) {
            this.delegate = delegate;
            cleanable = CLEANER.register(this, new State(delegate));
        }

        @Override
        public Id getId() {
            return delegate.getId();
        }

        @Override
        public void close() {
            delegate.close();
            cleanable.clean();
        }

        @Override
        public double value() {
            return delegate.value();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (GaugeWrapper) obj;
            return Objects.equals(this.delegate, that.delegate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(delegate);
        }

        @Override
        public String toString() {
            return "GaugeWrapper[delegate=" + delegate + "]";
        }
    }

    private static final class SummaryWrapper implements DistributionSummary {
        private final DistributionSummary delegate;
        private final Cleaner.Cleanable cleanable;

        private SummaryWrapper(DistributionSummary delegate) {
            this.delegate = delegate;
            cleanable = CLEANER.register(this, new State(delegate));
        }

        @Override
        public void record(double amount) {
            delegate.record(amount);
        }

        @Override
        public long count() {
            return delegate.count();
        }

        @Override
        public double totalAmount() {
            return delegate.totalAmount();
        }

        @Override
        public double max() {
            return delegate.max();
        }

        @Override
        public HistogramSnapshot takeSnapshot() {
            return delegate.takeSnapshot();
        }

        @Override
        public Id getId() {
            return delegate.getId();
        }

        @Override
        public void close() {
            delegate.close();
            cleanable.clean();
        }
    }
}
