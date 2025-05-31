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

package org.apache.bifromq.mqtt.handler;

import java.util.concurrent.TimeUnit;

/**
 * simple mps throttler with 'hard' maximum mps limit, mps could be reset.
 */
public class MPSThrottler {
    private int maxMPS;
    private long lastSec = 0;
    private long count = 0;

    public MPSThrottler(int maxMPS) {
        reset(maxMPS);
    }

    public int rateLimit() {
        return maxMPS;
    }

    public boolean pass() {
        long currentSec = upTimeInSec();
        if (lastSec == currentSec) {
            // same second bucket
            return ++count < maxMPS;
        } else {
            // new second bucket
            count = 0;
            lastSec = currentSec;
            return true;
        }
    }

    public void reset(int maxMPS) {
        assert maxMPS > 0;
        this.maxMPS = maxMPS;
    }

    private long upTimeInSec() {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    }
}
