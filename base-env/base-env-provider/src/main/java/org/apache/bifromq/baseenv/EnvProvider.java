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

import org.apache.bifromq.basehookloader.BaseHookLoader;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The class is served as the single and default entry point for getting threads at runtime. It's allowed to customize
 * the behavior by providing an implementation of IEnvProvider. If multiple implementations found in classpath, only the
 * first loaded one will be used, which is non-deterministic.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public final class EnvProvider implements IEnvProvider {
    public static final IEnvProvider INSTANCE;

    static {
        Map<String, IEnvProvider> envProviderMap = BaseHookLoader.load(IEnvProvider.class);
        if (envProviderMap.isEmpty()) {
            log.info("No custom env provider found, fallback to default behavior");
            INSTANCE = new EnvProvider();
        } else {
            Map.Entry<String, IEnvProvider> firstFound = envProviderMap.entrySet().stream().findFirst().get();
            log.info("Custom env provider is loaded: {}", firstFound.getKey());
            INSTANCE = firstFound.getValue();
        }
    }

    /**
     * Getting the number of available processors, if no customized implementation provided, it's same as
     * {@link Runtime#availableProcessors()}
     *
     * @return the number of available processors
     */
    @Override
    public int availableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Create a thread factory with given parameters
     *
     * @param name     the name of the created thread, a sequence number will be appended if more than one thread
     *                 created
     * @param daemon   if created thread is daemon thread
     * @param priority the priority of created thread
     * @return the thread created
     */
    @Override
    public ThreadFactory newThreadFactory(String name, boolean daemon, int priority) {
        return new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                int s = seq.getAndIncrement();
                t.setName(s > 0 ? name + "-" + s : name);
                t.setDaemon(daemon);
                t.setPriority(priority);
                return t;
            }
        };
    }
}
