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

package org.apache.bifromq.inbox.store;

import static org.apache.bifromq.basekv.utils.BoundaryUtil.inRange;
import static org.apache.bifromq.inbox.store.canon.TenantIdCanon.TENANT_ID_INTERNER;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.inbox.storage.proto.InboxMetadata;

@Slf4j
class InboxMetaCache implements IInboxMetaCache {
    private final Cache<CacheKey, InboxMetadata> cache;

    InboxMetaCache(Duration expireAfterAccess) {
        this.cache = Caffeine.newBuilder()
            .expireAfterAccess(expireAfterAccess)
            .build();
    }

    @Override
    public Optional<InboxMetadata> get(String tenantId, String inboxId, long incarnation,
                                       InboxMetadataProvider provider) {
        return Optional.ofNullable(cache.get(new CacheKey(TENANT_ID_INTERNER.intern(tenantId), inboxId, incarnation),
            key -> provider.get(key.tenantId, key.inboxId, key.incarnation)));
    }

    @Override
    public void upsert(String tenantId, InboxMetadata metadata) {
        cache.put(new CacheKey(TENANT_ID_INTERNER.intern(tenantId), metadata.getInboxId(),
            metadata.getIncarnation()), metadata);
    }

    @Override
    public void remove(String tenantId, String inboxId, long incarnation) {
        cache.invalidate(new CacheKey(TENANT_ID_INTERNER.intern(tenantId), inboxId, incarnation));
    }

    @Override
    public void reset(Boundary boundary) {
        for (CacheKey key : cache.asMap().keySet()) {
            if (!inRange(inboxStartKeyPrefix(key.tenantId, key.inboxId), boundary)) {
                cache.invalidate(key);
            }
        }
    }

    @Override
    public void close() {
        cache.invalidateAll();
    }

    private record CacheKey(String tenantId, String inboxId, long incarnation) {

    }
}
