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
import static org.apache.bifromq.basekv.utils.BoundaryUtil.upperBound;
import static org.apache.bifromq.inbox.store.canon.TenantIdCanon.TENANT_ID_INTERNER;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.isInboxInstanceKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.isInboxInstanceStartKey;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.parseInboxInstanceStartKeyPrefix;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Duration;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.basekv.store.api.IKVIterator;
import org.apache.bifromq.basekv.store.api.IKVReader;
import org.apache.bifromq.inbox.storage.proto.InboxMetadata;

@Slf4j
class InboxMetaCache implements IInboxMetaCache {
    private final Cache<CacheKey, SortedMap<Long, InboxMetadata>> cache;

    InboxMetaCache(Duration expireAfterAccess) {
        this.cache = Caffeine.newBuilder()
            .expireAfterAccess(expireAfterAccess)
            .build();
    }

    @Override
    public SortedMap<Long, InboxMetadata> get(String tenantId, String inboxId, IKVReader reader) {
        return cache.get(new CacheKey(TENANT_ID_INTERNER.intern(tenantId), inboxId),
            key -> getFromStore(key.tenantId, key.inboxId, reader));
    }

    @Override
    public Optional<InboxMetadata> get(String tenantId, String inboxId, long incarnation, IKVReader reader) {
        SortedMap<Long, InboxMetadata> inboxInstances = get(tenantId, inboxId, reader);
        return Optional.ofNullable(inboxInstances.get(incarnation));
    }

    @Override
    public boolean upsert(String tenantId, InboxMetadata metadata, IKVReader reader) {
        SortedMap<Long, InboxMetadata> inboxInstances = get(tenantId, metadata.getInboxId(), reader);
        boolean isNew = inboxInstances.isEmpty();
        inboxInstances.put(metadata.getIncarnation(), metadata);
        return isNew;
    }

    @Override
    public boolean remove(String tenantId, String inboxId, long incarnation, IKVReader reader) {
        SortedMap<Long, InboxMetadata> inboxInstances = get(tenantId, inboxId, reader);
        inboxInstances.remove(incarnation);
        if (inboxInstances.isEmpty()) {
            cache.invalidate(new CacheKey(tenantId, inboxId));
            return true;
        }
        return false;
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

    private SortedMap<Long, InboxMetadata> getFromStore(String tenantId, String inboxId, IKVReader reader) {
        reader.refresh();
        IKVIterator itr = reader.iterator();
        int probe = 0;
        SortedMap<Long, InboxMetadata> inboxInstances = new TreeMap<>();
        ByteString inboxStartKey = inboxStartKeyPrefix(tenantId, inboxId);
        for (itr.seek(inboxStartKey); itr.isValid(); ) {
            if (itr.key().startsWith(inboxStartKey)) {
                if (isInboxInstanceStartKey(itr.key())) {
                    probe = 0;
                    try {
                        InboxMetadata inboxMetadata = InboxMetadata.parseFrom(itr.value());
                        inboxInstances.put(inboxMetadata.getIncarnation(), inboxMetadata);
                    } catch (InvalidProtocolBufferException e) {
                        log.error("Unexpected error", e);
                    } finally {
                        itr.next();
                        probe++;
                    }
                } else {
                    if (probe < 20) {
                        itr.next();
                        probe++;
                    } else {
                        if (isInboxInstanceKey(itr.key())) {
                            itr.seek(upperBound(parseInboxInstanceStartKeyPrefix(itr.key())));
                        } else {
                            itr.next();
                            probe++;
                        }
                    }
                }
            } else {
                break;
            }
        }
        return inboxInstances;
    }

    private record CacheKey(String tenantId, String inboxId) {

    }
}
