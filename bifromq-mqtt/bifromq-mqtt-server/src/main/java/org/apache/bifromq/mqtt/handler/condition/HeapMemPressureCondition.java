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

package org.apache.bifromq.mqtt.handler.condition;

import org.apache.bifromq.baseenv.MemUsage;
import org.apache.bifromq.sysprops.props.IngressSlowDownHeapMemoryUsage;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HeapMemPressureCondition implements Condition {
    public static final HeapMemPressureCondition INSTANCE = new HeapMemPressureCondition();
    private static final double MAX_HEAP_MEMORY_USAGE = IngressSlowDownHeapMemoryUsage.INSTANCE.get();

    public boolean meet() {
        return MemUsage.local().heapMemoryUsage() > MAX_HEAP_MEMORY_USAGE;
    }

    @Override
    public String toString() {
        return "HighHeapMemoryUsage";
    }
}
