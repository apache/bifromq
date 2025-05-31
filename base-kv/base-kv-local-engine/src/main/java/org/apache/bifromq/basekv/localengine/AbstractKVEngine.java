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

package org.apache.bifromq.basekv.localengine;

import static com.google.common.collect.Lists.newArrayList;

import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.logger.SiftLogger;
import com.google.common.collect.Iterables;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;

/**
 * The abstract class of KVEngine.
 *
 * @param <T> the type of KV space created by the engine
 * @param <C> the type of configurator
 */
public abstract class AbstractKVEngine<T extends IKVSpace, C extends IKVEngineConfigurator> implements IKVEngine<T> {
    protected final String overrideIdentity;
    protected final C configurator;
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final Map<String, T> kvSpaceMap = new ConcurrentHashMap<>();
    protected Logger log;
    protected String[] metricTags;
    private Gauge gauge;

    public AbstractKVEngine(String overrideIdentity, C configurator) {
        this.overrideIdentity = overrideIdentity;
        this.configurator = configurator;
    }

    @Override
    public final void start(String... tags) {
        if (state.compareAndSet(State.INIT, State.STARTING)) {
            try {
                log = SiftLogger.getLogger(this.getClass(), tags);
                metricTags = tags;
                doStart(tags);
                state.set(State.STARTED);
                gauge = Gauge.builder("basekv.le.ranges", this.spaces()::size)
                    .tags(tags)
                    .register(Metrics.globalRegistry);
                afterStart();
            } catch (Throwable e) {
                state.set(State.FATAL_FAILURE);
                throw e;
            }
        }
    }

    protected abstract void doStart(String... metricTags);

    protected void afterStart() {

    }

    @Override
    public final void stop() {
        assertStarted();
        if (state.compareAndSet(State.STARTED, State.STOPPING)) {
            try {
                doStop();
                Metrics.globalRegistry.remove(gauge);
            } finally {
                state.set(State.STOPPED);
            }
        }
    }

    protected void doStop() {
        kvSpaceMap.values().forEach(IKVSpace::close);
    }

    protected void assertStarted() {
        assert state.get() == State.STARTED : "Not started";
    }

    @Override
    public final Map<String, T> spaces() {
        assertStarted();
        return Collections.unmodifiableMap(kvSpaceMap);
    }

    @Override
    public final T createIfMissing(String spaceId) {
        assertStarted();
        return kvSpaceMap.computeIfAbsent(spaceId,
            k -> {
                T space = buildKVSpace(spaceId, configurator, () -> kvSpaceMap.remove(spaceId), metricTags);
                space.open();
                return space;
            });
    }

    protected final void load(String spaceId) {
        T space = buildKVSpace(spaceId, configurator, () -> kvSpaceMap.remove(spaceId), metricTags);
        space.open();
        T prev = kvSpaceMap.put(spaceId, space);
        assert prev == null;
    }

    private T buildKVSpace(String spaceId, C configurator, Runnable onDestroy, String... tags) {
        String[] tagList =
            newArrayList(Iterables.concat(List.of(tags), List.of("spaceId", spaceId))).toArray(String[]::new);
        KVSpaceOpMeters opMeters = new KVSpaceOpMeters(spaceId, Tags.of(tagList));
        Logger logger = SiftLogger.getLogger("space.logger", tagList);
        return doBuildKVSpace(spaceId, configurator, onDestroy, opMeters, logger, tagList);
    }

    protected abstract T doBuildKVSpace(String spaceId,
                                        C configurator,
                                        Runnable onDestroy,
                                        KVSpaceOpMeters opMeters,
                                        Logger logger,
                                        String... tags);

    private enum State {
        INIT, STARTING, STARTED, FATAL_FAILURE, STOPPING, STOPPED
    }
}
