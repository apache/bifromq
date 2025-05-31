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

package org.apache.bifromq.basecluster.transport;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.InetSocketAddress;
import org.testng.annotations.Test;

public class TransportTest {
    @Test
    public void bindPort() {
        Transport transport = Transport.builder().build();
        int port = transport.bindAddress().getPort();
        transport.shutdown().join();

        transport = Transport.builder()
            .bindAddr(new InetSocketAddress(port))
            .build();
        assertEquals(transport.bindAddress().getPort(), port);
    }

    @Test
    public void bindEphemeralPort() {
        Transport transport = Transport.builder().build();
        assertTrue(transport.bindAddress().getPort() > 0);
    }
}
