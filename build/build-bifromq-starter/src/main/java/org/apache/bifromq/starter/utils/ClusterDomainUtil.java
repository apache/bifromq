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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class ClusterDomainUtil {
    public static CompletableFuture<InetAddress[]> resolve(String clusterDomainName, Duration timeout) {
        CompletableFuture<InetAddress[]> future = new CompletableFuture<>();
        Executor delayedExecutor = CompletableFuture.delayedExecutor(timeout.toNanos() / 3, TimeUnit.NANOSECONDS);
        Runnable resolveTask = new Runnable() {
            private final long startTime = System.nanoTime();

            @Override
            public void run() {
                if (!future.isDone()) {
                    try {
                        InetAddress[] addresses = InetAddress.getAllByName(clusterDomainName);
                        if (addresses.length > 0) {
                            future.complete(addresses);
                        } else {
                            throw new UnknownHostException("No addresses resolved for " + clusterDomainName);
                        }
                    } catch (UnknownHostException e) {
                        if (System.nanoTime() - startTime > timeout.toNanos()) {
                            future.completeExceptionally(
                                new UnknownHostException("Timeout while resolving domain name to a list of addresses"));
                        } else {
                            CompletableFuture.runAsync(this, delayedExecutor);
                        }
                    }
                }
            }
        };

        CompletableFuture.runAsync(resolveTask);
        return future;
    }
}
