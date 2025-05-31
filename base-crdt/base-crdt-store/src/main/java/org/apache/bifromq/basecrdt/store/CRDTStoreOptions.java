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

package org.apache.bifromq.basecrdt.store;

import org.apache.bifromq.baseenv.EnvProvider;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Builder(toBuilder = true)
@Accessors(chain = true, fluent = true)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class CRDTStoreOptions {
    @Builder.Default
    private String id = UUID.randomUUID().toString();
    @Builder.Default
    private int maxEventsInDelta = 1024;
    @Builder.Default
    private CompressAlgorithm compressAlgorithm = CompressAlgorithm.GZIP;
    @Builder.Default
    private Duration orHistoryExpireTime = Duration.ofSeconds(20);
    @Builder.Default
    private Duration inflationInterval = Duration.ofMillis(200);
    @Builder.Default
    private Duration maxCompactionTime = Duration.ofMillis(200);

    @Builder.Default
    private ScheduledExecutorService storeExecutor =
        new ScheduledThreadPoolExecutor(Math.max(2, EnvProvider.INSTANCE.availableProcessors() / 20),
            EnvProvider.INSTANCE.newThreadFactory("crdt-executor", true));
}
