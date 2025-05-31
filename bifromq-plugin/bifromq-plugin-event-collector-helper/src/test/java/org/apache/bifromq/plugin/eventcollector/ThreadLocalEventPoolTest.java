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

import static org.testng.Assert.assertNotSame;

import org.apache.bifromq.plugin.eventcollector.mqttbroker.PingReq;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class ThreadLocalEventPoolTest {
    @Test
    public void getLocal() {
        PingReq pingReq = ThreadLocalEventPool.getLocal(PingReq.class);
        AtomicReference<PingReq> pingReqRef = new AtomicReference<>();
        Thread t = new Thread(() -> pingReqRef.set(pingReqRef.get()));
        t.start();
        Uninterruptibles.joinUninterruptibly(t);
        assertNotSame(pingReqRef.get(), pingReq);
    }
}
