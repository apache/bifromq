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

package org.apache.bifromq.plugin.authprovider;

import static org.apache.bifromq.plugin.authprovider.MetricConstants.CALL_FAIL_COUNTER;
import static org.apache.bifromq.plugin.authprovider.MetricConstants.CALL_TIMER;
import static org.apache.bifromq.plugin.authprovider.MetricConstants.TAG_METHOD;
import static org.apache.bifromq.plugin.authprovider.MetricConstants.TAG_TYPE;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

class MetricManager {
    final Timer authCallTimer;
    final Counter authCallErrorCounter;
    final Timer extAuthCallTimer;
    final Counter extAuthCallErrorCounter;
    final Timer checkCallTimer;
    final Counter checkCallErrorCounter;

    MetricManager(String id) {
        authCallTimer = Timer.builder(CALL_TIMER)
            .tag(TAG_METHOD, "AuthProvider/auth")
            .tag(TAG_TYPE, id)
            .register(Metrics.globalRegistry);

        authCallErrorCounter = Counter.builder(CALL_FAIL_COUNTER)
            .tag(TAG_METHOD, "AuthProvider/auth")
            .tag(TAG_TYPE, id)
            .register(Metrics.globalRegistry);

        extAuthCallTimer = Timer.builder(CALL_TIMER)
            .tag(TAG_METHOD, "AuthProvider/extAuth")
            .tag(TAG_TYPE, id)
            .register(Metrics.globalRegistry);

        extAuthCallErrorCounter = Counter.builder(CALL_FAIL_COUNTER)
            .tag(TAG_METHOD, "AuthProvider/extAuth")
            .tag(TAG_TYPE, id)
            .register(Metrics.globalRegistry);

        checkCallTimer = Timer.builder(CALL_TIMER)
            .tag(TAG_METHOD, "AuthProvider/check")
            .tag(TAG_TYPE, id)
            .register(Metrics.globalRegistry);

        checkCallErrorCounter = Counter.builder(CALL_FAIL_COUNTER)
            .tag(TAG_METHOD, "AuthProvider/check")
            .tag(TAG_TYPE, id)
            .register(Metrics.globalRegistry);
    }

    void close() {
        Metrics.globalRegistry.remove(authCallTimer);
        Metrics.globalRegistry.remove(authCallErrorCounter);

        Metrics.globalRegistry.remove(extAuthCallTimer);
        Metrics.globalRegistry.remove(extAuthCallErrorCounter);

        Metrics.globalRegistry.remove(checkCallTimer);
        Metrics.globalRegistry.remove(checkCallErrorCounter);
    }
}
