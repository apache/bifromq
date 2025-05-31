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

package org.apache.bifromq.plugin.eventcollector;

import lombok.SneakyThrows;
import lombok.ToString;

@ToString
public abstract class Event<T extends Event<T>> implements Cloneable {
    private long hlc;

    public abstract EventType type();

    /**
     * The UTC timestamp of the event in milliseconds
     *
     * @return the timestamp
     */
    public long utc() {
        return hlc >>> 16;
    }

    /**
     * The timestamp from Hybrid Logical Clock, which is usually used for causal reasoning
     *
     * @return the hlc timestamp
     */
    public long hlc() {
        return hlc;
    }

    void hlc(long hlc) {
        this.hlc = hlc;
    }

    @SneakyThrows
    @Override
    public Object clone() {
        return super.clone();
    }

    public void clone(T orig) {
        this.hlc = orig.hlc();
    }
}
