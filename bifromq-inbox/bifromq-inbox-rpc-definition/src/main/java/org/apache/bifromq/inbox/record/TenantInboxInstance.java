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

package org.apache.bifromq.inbox.record;

import static org.apache.bifromq.inbox.util.InboxServiceUtil.parseReceiverId;

import org.apache.bifromq.type.MatchInfo;
import java.util.Comparator;

public record TenantInboxInstance(String tenantId, InboxInstance instance) implements Comparable<TenantInboxInstance> {
    private static final Comparator<TenantInboxInstance> COMPARATOR =
        Comparator.comparing(TenantInboxInstance::tenantId)
            .thenComparing(a -> a.instance().inboxId())
            .thenComparing(a -> a.instance().incarnation());

    public static TenantInboxInstance from(String tenantId, MatchInfo subInfo) {
        return new TenantInboxInstance(tenantId, parseReceiverId(subInfo.getReceiverId()));
    }

    public String receiverId() {
        return instance.receiverId();
    }

    @Override
    public int compareTo(TenantInboxInstance o) {
        return COMPARATOR.compare(this, o);
    }
}
