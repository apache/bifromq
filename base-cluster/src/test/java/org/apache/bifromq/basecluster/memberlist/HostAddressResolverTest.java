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

package org.apache.bifromq.basecluster.memberlist;

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.testng.annotations.Test;

public class HostAddressResolverTest {
    @Test
    public void unresolvedAddressShouldNotBeCached() {
        HostAddressResolver resolver = new HostAddressResolver(Duration.ofSeconds(1), Duration.ofSeconds(1));
        HostEndpoint host = HostEndpoint.newBuilder().setAddress("unresolved").setPort(1234).build();
        InetSocketAddress address = resolver.resolve(host);
        assertTrue(address.isUnresolved());
        assertNotSame(resolver.resolve(host), address);
    }

    @Test
    public void resolvedAddressShouldBeCached() {
        HostAddressResolver resolver = new HostAddressResolver(Duration.ofSeconds(1), Duration.ofSeconds(1));
        HostEndpoint host = HostEndpoint.newBuilder().setAddress("localhost").setPort(1234).build();
        InetSocketAddress address = resolver.resolve(host);
        assertFalse(address.isUnresolved());
        assertSame(resolver.resolve(host), address);
    }

    @Test
    public void resolvedAddressShouldBeRefreshed() {
        HostAddressResolver resolver = new HostAddressResolver(Duration.ofSeconds(1), Duration.ofMillis(100));
        HostEndpoint host = HostEndpoint.newBuilder().setAddress("localhost").setPort(1234).build();
        InetSocketAddress address = resolver.resolve(host);
        await().until(() -> resolver.resolve(host) != address);
    }
}
