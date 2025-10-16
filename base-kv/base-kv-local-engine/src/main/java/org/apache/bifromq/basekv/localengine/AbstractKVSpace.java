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

package org.apache.bifromq.basekv.localengine;

import static io.reactivex.rxjava3.subjects.BehaviorSubject.createDefault;
import static java.util.Collections.emptyMap;

import com.google.protobuf.ByteString;
import io.micrometer.core.instrument.Tags;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import org.apache.bifromq.basekv.proto.Boundary;
import org.slf4j.Logger;

/**
 * Base implementation of IKVSpace.
 *
 * @param <P> the type of epoch
 */
public abstract class AbstractKVSpace<P extends IKVSpaceEpoch> implements IKVSpace {
    protected final String id;
    protected final KVSpaceOpMeters opMeters;
    protected final Logger logger;
    protected final Tags tags;
    private final AtomicReference<State> state;
    private final Runnable onDestroy;
    private final BehaviorSubject<Map<ByteString, ByteString>> metadataSubject;

    public AbstractKVSpace(String id,
                           Runnable onDestroy,
                           KVSpaceOpMeters opMeters,
                           Logger logger,
                           String... tags) {
        this.id = id;
        this.opMeters = opMeters;
        this.logger = logger;
        this.onDestroy = onDestroy;
        state = new AtomicReference<>(State.Init);
        metadataSubject = createDefault(emptyMap());
        this.tags = Tags.of(tags).and("spaceId", id);
    }

    @Override
    public final String id() {
        return id;
    }

    @Override
    public final void open() {
        if (state.compareAndSet(State.Init, State.Opening)) {
            doOpen();
        }
    }

    @Override
    public final Observable<Map<ByteString, ByteString>> metadata() {
        return metadataSubject;
    }

    @Override
    public final KVSpaceDescriptor describe() {
        return new KVSpaceDescriptor(id, collectStats());
    }

    @Override
    public final long size() {
        return opMeters.sizeCallTimer.record(() -> doSize(Boundary.getDefaultInstance()));
    }

    @Override
    public final void destroy() {
        close();
        if (state.compareAndSet(State.Closed, State.Destroying)) {
            try {
                doDestroy();
            } catch (Throwable e) {
                throw new KVEngineException("Destroy KVRange error", e);
            } finally {
                onDestroy.run();
                state.set(State.Terminated);
            }
        }
    }

    @Override
    public final void close() {
        if (state.compareAndSet(State.Opening, State.Closing)) {
            try {
                doClose();
                metadataSubject.onComplete();
            } finally {
                state.set(State.Closed);
            }
        }
    }

    protected Map<ByteString, ByteString> currentMetadata() {
        return metadataSubject.getValue();
    }

    protected void updateMetadata(Map<ByteString, ByteString> newMetadata) {
        metadataSubject.onNext(newMetadata);
    }

    protected final State state() {
        return state.get();
    }

    protected abstract void doClose();

    protected void doDestroy() {
    }

    protected abstract P handle();

    protected abstract void doOpen();

    protected abstract long doSize(Boundary boundary);

    private Map<String, Double> collectStats() {
        Map<String, Double> stats = new HashMap<>();
        stats.put("size", (double) size());
        // TODO: more stats
        return stats;
    }

    protected enum State {
        Init, Opening, Destroying, Closing, Closed, Terminated
    }
}
