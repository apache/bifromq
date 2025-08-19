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

package org.apache.bifromq.basekv.metaservice;

import com.google.protobuf.Struct;
import io.reactivex.rxjava3.core.Observable;
import java.util.concurrent.CompletableFuture;

/**
 * The store balancer states reporter of base-kv cluster.
 */
public interface IBaseKVStoreBalancerStatesReporter {
    /**
     * Report the latest BalancerStates of local customized balancers.
     *
     * @param balancerFactoryClassFQN the fully qualified class name of the balancer factory
     * @param disable  if the balancer is disabled
     * @param loadRules the LoadRules in JSON object
     * @return the future of reporting
     */
    CompletableFuture<Void> reportBalancerState(String balancerFactoryClassFQN, boolean disable, Struct loadRules);

    /**
     * A signal to refresh the reporter's state.
     *
     * @return an observable that emits a timestamp when the reporter should refresh its state
     */
    Observable<Long> refreshSignal();

    /**
     * Stop the reporter.
     */
    void stop();
}
