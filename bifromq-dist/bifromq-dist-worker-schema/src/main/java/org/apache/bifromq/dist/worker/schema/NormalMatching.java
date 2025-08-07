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

package org.apache.bifromq.dist.worker.schema;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.bifromq.type.MatchInfo;
import org.apache.bifromq.type.RouteMatcher;

/**
 * Represent a normal matching route.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class NormalMatching extends Matching {
    private final String receiverUrl;

    private final long incarnation;

    @EqualsAndHashCode.Exclude
    private final MatchInfo matchInfo;

    @EqualsAndHashCode.Exclude
    private final KVSchemaUtil.Receiver receiver;

    NormalMatching(String tenantId, RouteMatcher matcher, String receiverUrl, long incarnation) {
        super(tenantId, matcher);
        this.receiverUrl = receiverUrl;
        this.receiver = KVSchemaUtil.parseReceiver(receiverUrl);
        this.incarnation = incarnation;

        matchInfo = MatchInfo.newBuilder()
            .setMatcher(matcher)
            .setReceiverId(receiver.receiverId())
            .setIncarnation(incarnation)
            .build();
    }

    @Override
    public Type type() {
        return Type.Normal;
    }

    public MatchInfo matchInfo() {
        return matchInfo;
    }

    public String receiverUrl() {
        return receiverUrl;
    }

    public int subBrokerId() {
        return receiver.subBrokerId();
    }

    public String delivererKey() {
        return receiver.delivererKey();
    }

    public long incarnation() {
        return incarnation;
    }
}
