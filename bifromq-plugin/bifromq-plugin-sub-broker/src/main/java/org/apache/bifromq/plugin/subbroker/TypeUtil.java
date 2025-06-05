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
import java.util.Map;
import java.util.stream.Collectors;

public class TypeUtil {
    public static Map<String, Map<MatchInfo, DeliveryResult.Code>> toMap(Map<String, DeliveryResults> deliveryResults) {
        return deliveryResults.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> to(entry.getValue())));
    }

    public static Map<String, DeliveryResults> toResult(
        Map<String, Map<MatchInfo, DeliveryResult.Code>> deliveryResults) {
        return deliveryResults.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> to(entry.getValue())));
    }

    public static Map<MatchInfo, DeliveryResult.Code> to(DeliveryResults deliveryResults) {
        return deliveryResults.getResultList().stream()
            .collect(Collectors.toMap(DeliveryResult::getMatchInfo, DeliveryResult::getCode));
    }

    public static DeliveryResults to(Map<MatchInfo, DeliveryResult.Code> deliveryResults) {
        DeliveryResults.Builder resultsBuilder = DeliveryResults.newBuilder();
        deliveryResults.forEach((matchInfo, code) -> resultsBuilder.addResult(
            DeliveryResult.newBuilder().setMatchInfo(matchInfo).setCode(code).build()));
        return resultsBuilder.build();
    }
}
