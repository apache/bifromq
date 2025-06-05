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

package org.apache.bifromq.basescheduler.spi;

import java.util.concurrent.CompletableFuture;

/**
 * A call scheduler is used for scheduling call before actually being submitted for batching.
 *
 * @param <ReqT> the request type
 */
public interface ICallScheduler<ReqT> {
    /**
     * Schedule a call to be called in the future when the returned future completed. The returned future could be
     * completed with DropException
     *
     * @param request the request to be scheduled
     * @return the future of the request to be invoked
     */
    default CompletableFuture<ReqT> submit(ReqT request) {
        return CompletableFuture.completedFuture(request);
    }

    /**
     * Close the call scheduler.
     */
    default void close() {
    }
}
