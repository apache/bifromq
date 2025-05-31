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

package org.apache.bifromq.baseenv;

import java.util.concurrent.ThreadFactory;

public class TestEnvProvider implements IEnvProvider {
    @Override
    public int availableProcessors() {
        return 1;
    }

    @Override
    public ThreadFactory newThreadFactory(String name, boolean daemon, int priority) {
        return r -> {
            Thread t = new Thread(r);
            t.setName(name);
            t.setDaemon(daemon);
            t.setPriority(priority);
            return t;
        };
    }
}
