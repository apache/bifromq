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

import static org.apache.bifromq.basekv.utils.BoundaryUtil.toBoundary;
import static org.apache.bifromq.inbox.store.schema.KVSchemaUtil.inboxStartKeyPrefix;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Optional;
import org.apache.bifromq.basekv.proto.Boundary;
import org.apache.bifromq.inbox.storage.proto.InboxMetadata;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxMetaCacheTest {
    private InboxMetaCache cache;
    private IInboxMetaCache.InboxMetadataProvider provider;

    @BeforeMethod
    public void setup() {
        cache = new InboxMetaCache(Duration.ofSeconds(60));
        provider = Mockito.mock(IInboxMetaCache.InboxMetadataProvider.class);
    }

    @Test
    public void loadAndCacheHit() {
        String tenant = "t1";
        String inbox = "i1";

        InboxMetadata meta1 = InboxMetadata.newBuilder().setInboxId(inbox).setIncarnation(1).build();
        InboxMetadata meta2 = InboxMetadata.newBuilder().setInboxId(inbox).setIncarnation(2).build();
        when(provider.get(eq(tenant), eq(inbox), eq(1L))).thenReturn(meta1);
        when(provider.get(eq(tenant), eq(inbox), eq(2L))).thenReturn(meta2);

        Optional<InboxMetadata> m1a = cache.get(tenant, inbox, 1, provider);
        assertTrue(m1a.isPresent());
        verify(provider, times(1)).get(eq(tenant), eq(inbox), eq(1L));

        Optional<InboxMetadata> m1b = cache.get(tenant, inbox, 1, provider);
        assertTrue(m1b.isPresent());
        verify(provider, times(1)).get(eq(tenant), eq(inbox), eq(1L)); // hit cache

        Optional<InboxMetadata> m2 = cache.get(tenant, inbox, 2, provider);
        assertTrue(m2.isPresent());
        verify(provider, times(1)).get(eq(tenant), eq(inbox), eq(2L));
    }

    @Test
    public void getByIncarnation() {
        String tenant = "t1";
        String inbox = "i1";

        InboxMetadata meta100 = InboxMetadata.newBuilder().setInboxId(inbox).setIncarnation(100).build();
        when(provider.get(eq(tenant), eq(inbox), eq(100L))).thenReturn(meta100);
        when(provider.get(eq(tenant), eq(inbox), eq(300L))).thenReturn(null);

        Optional<InboxMetadata> m100 = cache.get(tenant, inbox, 100, provider);
        Optional<InboxMetadata> m300 = cache.get(tenant, inbox, 300, provider);
        assertTrue(m100.isPresent());
        assertFalse(m300.isPresent());
        verify(provider, times(1)).get(eq(tenant), eq(inbox), eq(100L));
        verify(provider, times(1)).get(eq(tenant), eq(inbox), eq(300L));
    }

    @Test
    public void notCacheNullResult() {
        String tenant = "t1";
        String inbox = "i1";

        when(provider.get(eq(tenant), eq(inbox), eq(1L))).thenReturn(null);

        Optional<InboxMetadata> r1 = cache.get(tenant, inbox, 1, provider);
        Optional<InboxMetadata> r2 = cache.get(tenant, inbox, 1, provider);
        assertFalse(r1.isPresent());
        assertFalse(r2.isPresent());
        // provider should be called for both since null is not cached
        verify(provider, times(2)).get(eq(tenant), eq(inbox), eq(1L));
    }

    @Test
    public void upsertAndRemove() {
        String tenant = "t1";
        String inbox = "i1";

        InboxMetadata meta1 = InboxMetadata.newBuilder().setInboxId(inbox).setIncarnation(1).build();
        cache.upsert(tenant, meta1);

        Optional<InboxMetadata> cached = cache.get(tenant, inbox, 1, provider);
        assertTrue(cached.isPresent());
        verify(provider, times(0)).get(eq(tenant), eq(inbox), eq(1L)); // served by cache

        cache.remove(tenant, inbox, 1);

        when(provider.get(eq(tenant), eq(inbox), eq(1L))).thenReturn(null);
        Optional<InboxMetadata> afterRemove = cache.get(tenant, inbox, 1, provider);
        assertFalse(afterRemove.isPresent());
        verify(provider, times(1)).get(eq(tenant), eq(inbox), eq(1L));
    }

    @Test
    public void resetInvalidatesOutsideBoundary() {
        String tenant = "t1";
        String inbox1 = "i1";
        String inbox2 = "i2";

        InboxMetadata m1 = InboxMetadata.newBuilder().setInboxId(inbox1).setIncarnation(1).build();
        InboxMetadata m2 = InboxMetadata.newBuilder().setInboxId(inbox2).setIncarnation(2).build();
        when(provider.get(eq(tenant), eq(inbox1), eq(1L))).thenReturn(m1);
        when(provider.get(eq(tenant), eq(inbox2), eq(2L))).thenReturn(m2);

        assertTrue(cache.get(tenant, inbox1, 1, provider).isPresent());
        assertTrue(cache.get(tenant, inbox2, 2, provider).isPresent());
        verify(provider, times(1)).get(eq(tenant), eq(inbox1), eq(1L));
        verify(provider, times(1)).get(eq(tenant), eq(inbox2), eq(2L));

        ByteString start = inboxStartKeyPrefix(tenant, inbox1);
        ByteString end = start.concat(ByteString.copyFrom(new byte[] {(byte) 0xFF}));
        Boundary boundary = toBoundary(start, end);
        cache.reset(boundary);

        // inbox1 still cached
        assertTrue(cache.get(tenant, inbox1, 1, provider).isPresent());
        verify(provider, times(1)).get(eq(tenant), eq(inbox1), eq(1L));

        // inbox2 invalidated and reloaded
        assertTrue(cache.get(tenant, inbox2, 2, provider).isPresent());
        verify(provider, times(2)).get(eq(tenant), eq(inbox2), eq(2L));
    }
}
