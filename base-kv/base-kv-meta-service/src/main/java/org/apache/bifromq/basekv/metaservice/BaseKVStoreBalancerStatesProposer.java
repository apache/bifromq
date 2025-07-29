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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bifromq.basehlc.HLC;
import org.apache.bifromq.basekv.proto.BalancerStateSnapshot;

class BaseKVStoreBalancerStatesProposer implements IBaseKVStoreBalancerStatesProposer {
    private final IBaseKVStoreBalancerStatesProposalCRDT proposalCRDT;

    BaseKVStoreBalancerStatesProposer(IBaseKVStoreBalancerStatesProposalCRDT proposalCRDT) {
        this.proposalCRDT = proposalCRDT;
    }

    @Override
    public CompletableFuture<ProposalResult> proposeRunState(String balancerFactoryClassFQN, boolean disable) {
        BalancerStateSnapshot currentProposal = proposalCRDT.expectedBalancerState(balancerFactoryClassFQN)
            .orElse(BalancerStateSnapshot.getDefaultInstance());
        if (currentProposal.getDisable() == disable) {
            return CompletableFuture.completedFuture(ProposalResult.ACCEPTED);
        }
        long now = HLC.INST.get();
        BalancerStateSnapshot updatedState = BalancerStateSnapshot.newBuilder()
            .setDisable(disable)
            .setLoadRules(currentProposal.getLoadRules())
            .setHlc(now)
            .build();
        return proposeBalancerState(balancerFactoryClassFQN, updatedState);
    }

    @Override
    public CompletableFuture<ProposalResult> proposeLoadRules(String balancerFactoryClassFQN, Struct loadRules) {
        BalancerStateSnapshot currentProposal = proposalCRDT.expectedBalancerState(balancerFactoryClassFQN)
            .orElse(BalancerStateSnapshot.getDefaultInstance());
        if (currentProposal.getLoadRules().equals(loadRules)) {
            return CompletableFuture.completedFuture(ProposalResult.ACCEPTED);
        }
        long now = HLC.INST.get();
        BalancerStateSnapshot updatedState = BalancerStateSnapshot.newBuilder()
            .setDisable(currentProposal.getDisable())
            .setLoadRules(loadRules)
            .setHlc(now)
            .build();
        return proposeBalancerState(balancerFactoryClassFQN, updatedState);
    }

    @Override
    public CompletableFuture<Void> clearProposedState(String balancerFactoryClassFQN) {
        return proposalCRDT.removeBalancerState(balancerFactoryClassFQN);
    }

    @Override
    public void stop() {

    }

    private CompletableFuture<ProposalResult> proposeBalancerState(String balancerFactoryClass,
                                                                   BalancerStateSnapshot state) {
        CompletableFuture<ProposalResult> resultFuture = new CompletableFuture<>();
        long now = state.getHlc();
        proposalCRDT.expectedBalancerStates()
            .mapOptional(observed -> {
                if (!observed.containsKey(balancerFactoryClass)) {
                    return Optional.empty();
                }
                BalancerStateSnapshot effective = observed.get(balancerFactoryClass);
                if (effective.getHlc() < now) {
                    return Optional.empty();
                } else {
                    if (state.getDisable() == effective.getDisable()
                        && state.getLoadRules().equals(effective.getLoadRules())) {
                        return Optional.of(ProposalResult.ACCEPTED);
                    }
                    return Optional.of(ProposalResult.OVERRIDDEN);
                }
            })
            .take(1)
            .subscribe(resultFuture::complete, resultFuture::completeExceptionally);
        proposalCRDT.setBalancerState(balancerFactoryClass, state.getDisable(), state.getLoadRules());
        return resultFuture;
    }
}
