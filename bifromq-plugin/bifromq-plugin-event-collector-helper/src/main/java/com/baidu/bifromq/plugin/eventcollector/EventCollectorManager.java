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

package com.baidu.bifromq.plugin.eventcollector;


import com.baidu.bifromq.basehlc.HLC;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class EventCollectorManager implements IEventCollector {
    private static final EventPool ZERO_OUT_HOLDERS = new EventPool();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Map<String, IEventCollector> eventCollectors = new HashMap<>();
    private final Map<String, Timer> eventCollectorTimers = new HashMap<>();
    private final Counter callInvokeCounter;

    public EventCollectorManager(PluginManager pluginMgr) {

        for (IEventCollector eventCollector : pluginMgr.getExtensions(IEventCollector.class)) {
            log.info("Event collector loaded: {}", eventCollector.getClass().getName());
            eventCollectors.put(eventCollector.getClass().getName(), eventCollector);
            eventCollectorTimers.put(eventCollector.getClass().getName(), Timer.builder("call.exec.timer")
                .tag("method", "EventCollector/report")
                .tag("type", eventCollector.getClass().getSimpleName())
                .register(Metrics.globalRegistry));
        }
        callInvokeCounter = Counter.builder("event.collector.report.invoke.count")
            .register(Metrics.globalRegistry);
    }

    @Override
    public void report(Event<?> event) {
        callInvokeCounter.increment();
        event.hlc(HLC.INST.get());
        for (Map.Entry<String, IEventCollector> entry : eventCollectors.entrySet()) {
            Timer.Sample sample = Timer.start();
            try {
                entry.getValue().report(event);
            } catch (Throwable e) {
                log.warn("Failed to report event to collector: {}", entry.getKey());
            } finally {
                sample.stop(eventCollectorTimers.get(entry.getKey()));
            }
        }
        // clear out the event
        event.clone(ZERO_OUT_HOLDERS.get(event.type()));
    }


    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.debug("Closing event collector manager");
            eventCollectors.values().forEach(IEventCollector::close);
            eventCollectorTimers.values().forEach(Metrics.globalRegistry::remove);
            Metrics.globalRegistry.remove(callInvokeCounter);
            log.debug("Event collector manager closed");
        }
    }

    @VisibleForTesting
    IEventCollector get(String collectorName) {
        return eventCollectors.get(collectorName);
    }
}
