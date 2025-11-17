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

package org.apache.bifromq.baserpc.client.loadbalancer;

import static io.grpc.ConnectivityState.READY;
import static org.apache.bifromq.baserpc.client.exception.ExceptionUtil.SERVER_NOT_FOUND;
import static org.apache.bifromq.baserpc.client.exception.ExceptionUtil.SERVER_UNREACHABLE;

import io.grpc.LoadBalancer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SubChannelPicker extends LoadBalancer.SubchannelPicker {
    private final Map<String, ChannelList> serverChannels = new HashMap<>();

    public SubChannelPicker() {
    }

    public void refresh(Map<String, ChannelList> serverChannels) {
        this.serverChannels.clear();
        serverChannels.forEach((serverId, channelList) -> this.serverChannels.put(serverId, channelList.copy()));
    }

    @Override
    public LoadBalancer.PickResult pickSubchannel(LoadBalancer.PickSubchannelArgs pickSubchannelArgs) {
        String desiredServerId = pickSubchannelArgs.getHeaders().get(Constants.DESIRED_SERVER_META_KEY);
        if (desiredServerId != null && serverChannels.containsKey(desiredServerId)) {
            log.trace("Direct pick sub-channel by serverId:{}", desiredServerId);
            pickSubchannelArgs.getHeaders().remove(Constants.DESIRED_SERVER_META_KEY, desiredServerId);
            Optional<LoadBalancer.Subchannel> selectedSubChannel = getSubChannel(desiredServerId);
            return selectedSubChannel.map(LoadBalancer.PickResult::withSubchannel)
                .orElseGet(() -> LoadBalancer.PickResult.withError(SERVER_UNREACHABLE));
        }
        return LoadBalancer.PickResult.withError(SERVER_NOT_FOUND);
    }

    private Optional<LoadBalancer.Subchannel> getSubChannel(String serverId) {
        List<LoadBalancer.Subchannel> subChannels = serverChannels.get(serverId).subChannels.stream()
            .filter(sc -> sc.getAttributes().get(Constants.STATE_INFO).get().getState() == READY)
            .toList();
        if (subChannels.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(subChannels.get(ThreadLocalRandom.current().nextInt(subChannels.size())));
    }
}
