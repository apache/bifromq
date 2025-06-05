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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;
import lombok.SneakyThrows;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InboxFetcherRegistryTest {
    @Mock
    private IInboxFetcher fetcher1;
    @Mock
    private IInboxFetcher fetcher2;
    @Mock
    private IInboxFetcher fetcher3;

    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @SneakyThrows
    @AfterMethod
    public void teardown() {
        closeable.close();
    }

    @Test
    public void regAndUnReg() {
        InboxFetcherRegistry registry = new InboxFetcherRegistry();
        mockFetcher(fetcher1, "tenantA", "inbox1", "fetcher1");
        registry.reg(fetcher1);
        assertEquals(registry.get(fetcher1.tenantId(), fetcher1.delivererKey()), Collections.singleton(fetcher1));
        registry.unreg(fetcher1);
        assertTrue(registry.get(fetcher1.tenantId(), fetcher1.delivererKey()).isEmpty());
    }

    @Test
    public void kick() {
        InboxFetcherRegistry registry = new InboxFetcherRegistry();
        mockFetcher(fetcher1, "tenantA", "inbox1", "fetcher1");
        mockFetcher(fetcher2, "tenantA", "inbox1", "fetcher1");
        registry.reg(fetcher1);
        registry.reg(fetcher2);
        verify(fetcher1).close();
    }

    @Test
    public void iterator() {
        InboxFetcherRegistry registry = new InboxFetcherRegistry();
        mockFetcher(fetcher1, "tenantA", "inbox1", "fetcher1");
        mockFetcher(fetcher2, "tenantA", "inbox2", "fetcher2");
        mockFetcher(fetcher3, "tenantB", "inbox3", "fetcher3");
        registry.reg(fetcher1);
        registry.reg(fetcher2);
        registry.reg(fetcher3);
        Set<IInboxFetcher> fetchers = Sets.newHashSet(registry.iterator());
        assertEquals(fetchers, Sets.newHashSet(fetcher1, fetcher2, fetcher3));
    }

    private void mockFetcher(IInboxFetcher fetcher, String tenantId, String delivererKey, String fetcherId) {
        when(fetcher.tenantId()).thenReturn(tenantId);
        when(fetcher.delivererKey()).thenReturn(delivererKey);
        when(fetcher.id()).thenReturn(fetcherId);
    }
}
