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

package org.apache.bifromq.basescheduler;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for batch call.
 *
 * @param <ReqT>        the type of the request to be fulfilled in batch
 * @param <RespT>       the type of the response expected
 * @param <BatcherKeyT> the type of the key to identify the batch
 */
public interface IBatchCall<ReqT, RespT, BatcherKeyT> {
    /**
     * Add a call task to the batch.
     *
     * @param task the task to be fulfilled in the batch
     */
    void add(ICallTask<ReqT, RespT, BatcherKeyT> task);

    /**
     * Reset the batch call object to initial state to be reused again.
     *
     * @param abort if true, the batch tasks is reset due to abort by caller.
     */
    void reset(boolean abort);

    /**
     * Execute the async batch call.
     *
     * @return a future which will complete when batch is done
     */
    CompletableFuture<Void> execute();

    /**
     * Destroy the batch release any resources associated.
     */
    default void destroy() {
    }
}

