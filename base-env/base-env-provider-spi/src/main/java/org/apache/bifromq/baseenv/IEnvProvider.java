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

package org.apache.bifromq.baseenv;

import java.util.concurrent.ThreadFactory;

/**
 * This is a service provider interface(SPI) for customizing the thread running environment.
 */
public interface IEnvProvider {
    /**
     * Get current available processors number
     *
     * @return the available processors number
     */
    int availableProcessors();

    /**
     * Create a thread factory with given parameters
     *
     * @param name     the name of the created thread
     * @param daemon   if created thread is daemon thread
     * @param priority the priority of created thread
     * @return the created thread
     */
    ThreadFactory newThreadFactory(String name, boolean daemon, int priority);

    /**
     * Create a thread factory which creating thread with normal priority
     *
     * @param name   the name of the created thread
     * @param daemon if created thread is daemon thread
     * @return the created thread
     */
    default ThreadFactory newThreadFactory(String name, boolean daemon) {
        return newThreadFactory(name, daemon, Thread.NORM_PRIORITY);
    }

    /**
     * Create a thread factory which creating non-daemon thread with normal priority
     *
     * @param name the name of the created thread
     * @return the created thread
     */
    default ThreadFactory newThreadFactory(String name) {
        return newThreadFactory(name, false);
    }
}
