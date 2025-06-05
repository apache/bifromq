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

package org.apache.bifromq.plugin.subbroker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.PluginManager;

@Slf4j
public class SubBrokerManager implements ISubBrokerManager {
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Map<Integer, ISubBroker> receivers = new HashMap<>();

    public SubBrokerManager(PluginManager pluginMgr, ISubBroker... builtInReceivers) {
        for (ISubBroker receiver : builtInReceivers) {
            log.info("Register built-in receiver[{}] with id[{}]", receiver.getClass().getSimpleName(), receiver.id());
            receivers.put(receiver.id(), new MonitoredSubBroker(receiver));
        }
        List<ISubBroker> customReceivers = pluginMgr.getExtensions(ISubBroker.class);
        for (ISubBroker customReceiver : customReceivers) {
            if (receivers.containsKey(customReceiver.id())) {
                log.warn("Id[{}] is reserved for receiver[{}], skip registering custom receiver[{}]",
                    customReceiver.id(),
                    receivers.get(customReceiver.id()).getClass().getName(),
                    customReceiver.getClass().getName());
            } else {
                log.info("Register custom receiver[{}] with id[{}]",
                    customReceiver.getClass().getSimpleName(), customReceiver.id());
                receivers.put(customReceiver.id(), new MonitoredSubBroker(customReceiver));
            }
        }
    }

    @Override
    public ISubBroker get(int subBrokerId) {
        return receivers.getOrDefault(subBrokerId, NoInboxSubBroker.INSTANCE);
    }

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            log.debug("Stopping SubBrokerManager");
            receivers.values().forEach(ISubBroker::close);
            log.debug("SubBrokerManager stopped");
        }
    }
}
