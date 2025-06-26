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

package org.apache.bifromq.starter.utils;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public class ClusterDomainUtilTest {

    @Test
    public void testResolve() {
        Duration timeout = Duration.ofSeconds(5);
        CompletableFuture<InetAddress[]> future = ClusterDomainUtil.resolve("localhost", timeout);
        InetAddress[] addresses = future.join();
        assertTrue(addresses.length > 0);
    }

    @Test
    public void testResolveTimeout() {
        String domainName = "this.domain.must.be.non.existed";
        Duration timeout = Duration.ofMillis(100);
        assertThrows(ExecutionException.class, () -> ClusterDomainUtil.resolve(domainName, timeout).get());
    }
}