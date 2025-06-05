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

package org.apache.bifromq.plugin.subbroker;

import org.apache.bifromq.type.MatchInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class NoInboxSubBroker implements ISubBroker {
    public static final ISubBroker INSTANCE = new NoInboxSubBroker();

    @Override
    public int id() {
        return Integer.MIN_VALUE;
    }

    @Override
    public CompletableFuture<CheckReply> check(CheckRequest request) {
        CheckReply.Builder replyBuilder = CheckReply.newBuilder();
        for (MatchInfo matchInfo : request.getMatchInfoList()) {
            replyBuilder.addCode(CheckReply.Code.NO_RECEIVER);
        }
        return CompletableFuture.completedFuture(replyBuilder.build());
    }

    @Override
    public IDeliverer open(String delivererKey) {
        return new IDeliverer() {
            @Override
            public CompletableFuture<DeliveryReply> deliver(DeliveryRequest request) {
                DeliveryReply.Builder replyBuilder = DeliveryReply.newBuilder().setCode(DeliveryReply.Code.OK);
                for (Map.Entry<String, DeliveryPackage> entry : request.getPackageMap().entrySet()) {
                    Map<MatchInfo, DeliveryResult.Code> results = new HashMap<>();
                    for (DeliveryPack pack : entry.getValue().getPackList()) {
                        for (MatchInfo matchInfo : pack.getMatchInfoList()) {
                            results.put(matchInfo, DeliveryResult.Code.NO_SUB);
                        }
                    }
                    DeliveryResults.Builder resultsBuilder = DeliveryResults.newBuilder();
                    results.forEach((matchInfo, code) ->
                        resultsBuilder.addResult(DeliveryResult.newBuilder()
                            .setMatchInfo(matchInfo)
                            .setCode(code)
                            .build()));
                    replyBuilder.putResult(entry.getKey(), resultsBuilder.build());
                }
                return CompletableFuture.completedFuture(replyBuilder.build());
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public void close() {

    }
}
