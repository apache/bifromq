/*
 * Copyright (c) 2025. The BifroMQ Authors. All Rights Reserved.
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

package org.apache.bifromq.inbox.store.delay;

import java.util.Set;

/**
 * The interface of DelayTask Runner which is used to schedule tasks to be executed after a specified delay.
 *
 * @param <KeyT> the type of the key used to identify the task.
 */
public interface IDelayTaskRunner<KeyT> {
    /**
     * Schedule a new delayedTask.
     *
     * @param key         the key under monitoring
     * @param delayedTask the delayedTask to be triggered
     * @param <TaskT>     the type of the delayedTask
     */
    <TaskT extends IDelayedTask<KeyT>> void schedule(KeyT key, TaskT delayedTask);

    /**
     * Schedule a new delayedTask if the key is not already scheduled.
     *
     * @param key         the key under monitoring
     * @param delayedTask the delayedTask to be triggered
     * @param <TaskT>     the type of the delayedTask
     */
    <TaskT extends IDelayedTask<KeyT>> void scheduleIfAbsent(KeyT key, TaskT delayedTask);

    /**
     * Cancel the task associated with the specified key.
     *
     * @param keys the keys to cancel
     */
    void cancelAll(Set<KeyT> keys);

    /**
     * Shutdown the DelayTaskRunner.
     */
    void shutdown();
}
