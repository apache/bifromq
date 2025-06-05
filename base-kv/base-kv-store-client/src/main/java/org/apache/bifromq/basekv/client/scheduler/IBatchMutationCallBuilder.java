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

package org.apache.bifromq.basekv.client.scheduler;

import org.apache.bifromq.basekv.client.IMutationPipeline;

/**
 * Interface for building batch mutation calls.
 *
 * @param <ReqT> the type of the request
 * @param <RespT> the type of the response
 * @param <BatchCallT> the type of the batch call
 */
public interface IBatchMutationCallBuilder<ReqT, RespT, BatchCallT extends BatchMutationCall<ReqT, RespT>> {
    /**
     * Creates a new batch call.
     *
     * @param pipeline the mutation pipeline to use
     * @param batcherKey the key for the batcher
     * @return a new batch call
     */
    BatchCallT newBatchCall(IMutationPipeline pipeline, MutationCallBatcherKey batcherKey);
}
