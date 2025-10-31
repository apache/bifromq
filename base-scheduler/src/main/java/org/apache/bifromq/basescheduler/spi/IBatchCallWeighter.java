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

/**
 * SPI interface for estimating the weighted size of a BatchCall.
 *
 * @param <ReqT> the batched call type
 */
public interface IBatchCallWeighter<ReqT> {
    /**
     * Will be invoked by batcher when building batch call.
     *
     * @param req the batched call
     */
    void add(ReqT req);

    /**
     * The accumulated weighted size of the BatchCall.
     *
     * @return the weighted size
     */
    long weight();

    /**
     * Reset the weighter for next use.
     */
    void reset();
}
