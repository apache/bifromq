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

package org.apache.bifromq.basecluster.memberlist;

import org.apache.bifromq.basecluster.messenger.IRecipient;
import org.apache.bifromq.basecluster.messenger.IRecipientSelector;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MemberSelector implements IRecipientSelector {
    private final Disposable disposable;
    private volatile List<IRecipient> recipients = new ArrayList<>();

    public MemberSelector(IHostMemberList memberList, Scheduler scheduler, IHostAddressResolver addressResolver) {
        disposable = memberList.members()
            .observeOn(scheduler)
            .subscribe(members -> recipients = members.keySet()
                .stream()
                .map(m -> {
                    InetSocketAddress address = addressResolver.resolve(m);
                    return (IRecipient) () -> address;
                })
                .collect(Collectors.toList()));
    }

    @Override
    public Collection<? extends IRecipient> selectForSpread(int limit) {
        List<IRecipient> addresses = recipients;
        if (addresses.size() <= limit) {
            return addresses;
        }
        Collections.shuffle(addresses);
        return addresses.subList(0, limit);
    }

    @Override
    public int clusterSize() {
        return recipients.size();
    }

    public void stop() {
        disposable.dispose();
    }
}
