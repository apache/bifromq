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

package org.apache.bifromq.basescheduler;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for a task that can be batched.
 *
 * @param <CallT>       the type of the call
 * @param <CallResultT> the type of the call result
 * @param <BatcherKeyT> the type of the batcher key
 */
public interface ICallTask<CallT, CallResultT, BatcherKeyT> {

    /**
     * the call of the task.
     *
     * @return the call to be fulfilled
     */
    CallT call();

    /**
     * the promise of the result of the call.
     *
     * @return the promise of the result
     */
    CompletableFuture<CallResultT> resultPromise();

    /**
     * the key to batch the task.
     *
     * @return the key to batch the task
     */
    BatcherKeyT batcherKey();

    /**
     * the timestamp of the task.
     *
     * @return the timestamp
     */
    long ts();
}
