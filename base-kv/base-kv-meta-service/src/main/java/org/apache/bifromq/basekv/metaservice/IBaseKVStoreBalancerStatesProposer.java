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
import java.util.concurrent.CompletableFuture;

/**
 * The store balancer state proposer of base-kv cluster.
 */
public interface IBaseKVStoreBalancerStatesProposer {
    /**
     * Propose the run state for given balancer.
     *
     * @param balancerFactoryClassFQN the balancer factory class
     * @param disable            if the balancer should be disabled
     * @return the future of setting state
     */
    CompletableFuture<ProposalResult> proposeRunState(String balancerFactoryClassFQN, boolean disable);

    /**
     * Propose the LoadRules update for given balancer.
     *
     * @param balancerFactoryClassFQN the balancer factory class
     * @param loadRules            the LoadRules in JSON object
     * @return the future of setting LoadRules
     */
    CompletableFuture<ProposalResult> proposeLoadRules(String balancerFactoryClassFQN, Struct loadRules);

    /**
     *  Clear the proposed state for given balancer factory class.
     *
     * @param balancerFactoryClassFQN the balancer factory class
     * @return the future of clearing the proposed state
     */
    CompletableFuture<Void> clearProposedState(String balancerFactoryClassFQN);

    void stop();

    /**
     * The result of proposing LoadRules.
     */
    enum ProposalResult {
        ACCEPTED, OVERRIDDEN
    }
}