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

package org.apache.bifromq.basekv.localengine.memory;

import static com.google.protobuf.ByteString.unsignedLexicographicalComparator;

import org.apache.bifromq.basekv.localengine.IKVSpace;
import org.apache.bifromq.basekv.localengine.IKVSpaceWriter;
import org.apache.bifromq.basekv.localengine.ISyncContext;
import org.apache.bifromq.basekv.localengine.KVSpaceDescriptor;
import org.apache.bifromq.basekv.localengine.SyncContext;
import org.apache.bifromq.basekv.localengine.metrics.KVSpaceOpMeters;
import com.google.protobuf.ByteString;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;

class InMemKVSpace<E extends InMemKVEngine<E, T>, T extends InMemKVSpace<E, T>> extends InMemKVSpaceReader
    implements IKVSpace {
    protected final String id;
    protected final Map<ByteString, ByteString> metadataMap = new ConcurrentHashMap<>();
    protected final ConcurrentSkipListMap<ByteString, ByteString> rangeData =
        new ConcurrentSkipListMap<>(unsignedLexicographicalComparator());
    private final E engine;
    private final BehaviorSubject<Map<ByteString, ByteString>> metadataSubject = BehaviorSubject.create();
    private final ISyncContext syncContext = new SyncContext();
    protected final ISyncContext.IRefresher metadataRefresher = syncContext.refresher();
    private final Runnable onDestroy;

    protected InMemKVSpace(String id,
                           InMemKVEngineConfigurator configurator,
                           E engine,
                           Runnable onDestroy,
                           KVSpaceOpMeters readOpMeters,
                           Logger logger) {
        super(id, readOpMeters, logger);
        this.id = id;
        this.engine = engine;
        this.onDestroy = onDestroy;
    }

    ISyncContext syncContext() {
        return syncContext;
    }

    @Override
    protected Map<ByteString, ByteString> metadataMap() {
        return metadataRefresher.call(() -> metadataMap);
    }

    @Override
    protected ConcurrentSkipListMap<ByteString, ByteString> rangeData() {
        return rangeData;
    }


    @Override
    public Observable<Map<ByteString, ByteString>> metadata() {
        return metadataSubject;
    }

    @Override
    public KVSpaceDescriptor describe() {
        return new KVSpaceDescriptor(id, collectStats());
    }

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    private Map<String, Double> collectStats() {
        Map<String, Double> stats = new HashMap<>();
        stats.put("size", (double) size());
        // TODO: more stats
        return stats;
    }


    @Override
    public void destroy() {
        metadataSubject.onComplete();
        onDestroy.run();
    }


    @Override
    public IKVSpaceWriter toWriter() {
        return new InMemKVSpaceWriter<>(id, metadataMap, rangeData, engine, syncContext,
            metadataUpdated -> {
                if (metadataUpdated) {
                    this.loadMetadata();
                }
            }, opMeters, logger);
    }

    private void loadMetadata() {
        metadataRefresher.runIfNeeded(() -> {
            if (!metadataMap.isEmpty()) {
                metadataSubject.onNext(Collections.unmodifiableMap(new HashMap<>(metadataMap)));
            }
        });
    }
}
