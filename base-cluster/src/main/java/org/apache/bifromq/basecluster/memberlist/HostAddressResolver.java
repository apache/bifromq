/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.basecluster.memberlist;

import static com.github.benmanes.caffeine.cache.Scheduler.systemScheduler;

import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.checkerframework.checker.index.qual.NonNegative;

public class HostAddressResolver implements IHostAddressResolver {
    private final LoadingCache<HostEndpoint, InetSocketAddress> hostAddressCache;

    public HostAddressResolver(Duration expiryInterval, Duration refreshInterval) {
        hostAddressCache = Caffeine.newBuilder()
            .scheduler(systemScheduler())
            .expireAfter(new Expiry<HostEndpoint, InetSocketAddress>() {
                @Override
                public long expireAfterCreate(HostEndpoint key, InetSocketAddress value, long currentTime) {
                    if (value.isUnresolved()) {
                        // do not cache unresolved address
                        return 0;
                    }
                    return expiryInterval.toNanos();
                }

                @Override
                public long expireAfterUpdate(HostEndpoint key, InetSocketAddress value, long currentTime,
                                              @NonNegative long currentDuration) {
                    if (value.isUnresolved()) {
                        // do not cache unresolved address
                        return 0;
                    }
                    return expiryInterval.toNanos();
                }

                @Override
                public long expireAfterRead(HostEndpoint key, InetSocketAddress value, long currentTime,
                                            @NonNegative long currentDuration) {
                    return expiryInterval.toNanos();
                }
            })
            .refreshAfterWrite(refreshInterval)
            .build(key -> new InetSocketAddress(key.getAddress(), key.getPort()));
    }

    @Override
    public InetSocketAddress resolve(HostEndpoint endpoint) {
        return hostAddressCache.get(endpoint);
    }
}
