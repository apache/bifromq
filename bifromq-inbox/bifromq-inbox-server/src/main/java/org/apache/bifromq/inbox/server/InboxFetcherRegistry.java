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

package org.apache.bifromq.inbox.server;

import static java.util.Collections.emptyMap;

import com.google.common.collect.Iterators;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class InboxFetcherRegistry implements IInboxFetcherRegistry {
    private final ConcurrentMap<String, Map<String, Map<String, IInboxFetcher>>> fetchers = new ConcurrentHashMap<>();

    @Override
    public void reg(IInboxFetcher fetcher) {
        fetchers.compute(fetcher.tenantId(), (key, val) -> {
            if (val == null) {
                val = new ConcurrentHashMap<>();
            }
            IInboxFetcher prevFetcher = val.computeIfAbsent(fetcher.delivererKey(), k -> new ConcurrentHashMap<>())
                .put(fetcher.id(), fetcher);
            if (prevFetcher != null) {
                prevFetcher.close();
            }
            return val;
        });
    }

    @Override
    public void unreg(IInboxFetcher fetcher) {
        fetchers.compute(fetcher.tenantId(), (tenantId, m) -> {
            if (m != null) {
                m.computeIfPresent(fetcher.delivererKey(), (k, v) -> {
                    v.remove(fetcher.id(), fetcher);
                    if (v.isEmpty()) {
                        return null;
                    }
                    return v;
                });
                if (m.isEmpty()) {
                    return null;
                }
            }
            return m;
        });
    }

    @Override
    public Collection<IInboxFetcher> get(String tenantId, String delivererKey) {
        return fetchers.getOrDefault(tenantId, emptyMap()).getOrDefault(delivererKey, emptyMap()).values();
    }

    @Override
    public Iterator<IInboxFetcher> iterator() {
        return Iterators.concat(
            Iterators.transform(
                Iterators.concat(
                    fetchers.values().stream().map(m -> m.values().iterator()).iterator()
                ),
                e -> e.values().iterator()
            )
        );
    }
}
