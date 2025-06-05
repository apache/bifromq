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

import static org.apache.bifromq.baserpc.client.loadbalancer.Constants.IN_PROC_SERVER_ATTR_KEY;
import static org.apache.bifromq.baserpc.client.loadbalancer.Constants.SERVER_GROUP_TAG_ATTR_KEY;
import static org.apache.bifromq.baserpc.client.loadbalancer.Constants.SERVER_ID_ATTR_KEY;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.IDLE;
import static io.grpc.ConnectivityState.READY;
import static io.grpc.ConnectivityState.SHUTDOWN;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import org.apache.bifromq.baseenv.EnvProvider;
import com.google.common.collect.Maps;
import io.grpc.Attributes;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TrafficDirectiveLoadBalancer extends LoadBalancer {

    private final Helper helper;

    private final IServerSelectorUpdateListener updateListener;

    private final SubChannelPicker currentPicker;

    private final Map<String, ChannelList> serverChannels = Maps.newHashMap();

    private final AtomicBoolean balancingStateUpdateScheduled = new AtomicBoolean(false);

    private Map<String, Boolean> currentServers = Maps.newHashMap();
    private Map<String, Set<String>> currentServerGroupTags = Maps.newHashMap();
    private Map<String, Map<String, Integer>> currentTrafficDirective = Maps.newHashMap();

    TrafficDirectiveLoadBalancer(Helper helper, IServerSelectorUpdateListener updateListener) {
        this.helper = checkNotNull(helper, "helper");
        this.updateListener = updateListener;
        this.currentPicker = new SubChannelPicker();
    }

    @Override
    public void handleResolvedAddresses(ResolvedAddresses resolvedAddresses) {
        log.debug("Handle traffic change: resolvedAddresses={}", resolvedAddresses);
        Map<String, EquivalentAddressGroup> newResolved = new HashMap<>();
        Map<String, Boolean> newServers = new HashMap<>();
        for (EquivalentAddressGroup addressGroup : resolvedAddresses.getAddresses()) {
            String serverId = addressGroup.getAttributes().get(SERVER_ID_ATTR_KEY);
            newResolved.put(serverId, addressGroup);
            newServers.put(serverId, addressGroup.getAttributes().get(IN_PROC_SERVER_ATTR_KEY));
        }
        Map<String, Set<String>> newServerGroupTags = Maps.newHashMap();
        for (EquivalentAddressGroup addressGroup : resolvedAddresses.getAddresses()) {
            newServerGroupTags.put(addressGroup.getAttributes().get(SERVER_ID_ATTR_KEY),
                addressGroup.getAttributes().get(SERVER_GROUP_TAG_ATTR_KEY));
        }
        Map<String, Map<String, Integer>> newTrafficDirective = resolvedAddresses.getAttributes()
            .get(Constants.TRAFFIC_DIRECTIVE_ATTR_KEY);
        boolean updatePicker = !currentTrafficDirective.equals(newTrafficDirective)
            || !currentServerGroupTags.equals(newServerGroupTags);
        currentServers = newServers;
        currentServerGroupTags = newServerGroupTags;
        currentTrafficDirective = newTrafficDirective;
        int requested = Math.min(5, EnvProvider.INSTANCE.availableProcessors());
        Set<String> currentServers = serverChannels.keySet();
        Set<String> latestServers = newResolved.keySet();
        Set<String> addedServers = difference(latestServers, currentServers);
        Set<String> removedServers = difference(currentServers, latestServers);

        // make sure enough subchannelRegistry opened for existing servers.
        for (String serverId : currentServers) {
            if (!removedServers.contains(serverId)) {
                int openNow = serverChannels.get(serverId).subChannels.size();
                if (requested > openNow) {
                    updatePicker = true;
                    IntStream.range(0, requested - openNow).forEach(
                        i -> serverChannels.get(serverId).subChannels
                            .add(setupSubchannel(serverId, newResolved.get(serverId), newServers.get(serverId))));
                }
            }
        }

        // Create new subchannelRegistry for new servers.
        for (String serverId : addedServers) {
            serverChannels.computeIfAbsent(serverId, k -> {
                ChannelList scList = new ChannelList(newServers.get(serverId));
                IntStream.range(0, requested).forEach(i -> scList.subChannels.add(
                    setupSubchannel(serverId, newResolved.get(serverId), newServers.get(serverId))));
                return scList;
            });
        }

        ArrayList<ChannelList> removedSubchannels = new ArrayList<>();
        for (String serverId : removedServers) {
            removedSubchannels.add(serverChannels.remove(serverId));
        }

        // Shutdown removed subchannelRegistry
        for (ChannelList subChannelList : removedSubchannels) {
            for (Subchannel subchannel : subChannelList.subChannels) {
                shutdownSubChannel(subchannel);
            }
        }

        if (updatePicker) {
            // Update the picker before shutting down the subchannelRegistry, to reduce the chance of the race
            // between picking a subchannel and shutting it down.
            scheduleBalancingStateUpdate();
        }
    }

    @Override
    public void handleNameResolutionError(Status status) {
        log.error("Name resolution error:{}", status.getDescription());
        helper.updateBalancingState(TRANSIENT_FAILURE, currentPicker);
    }

    @Override
    public void shutdown() {
        log.debug("Shutting down all subchannels");
        for (ChannelList subChannelList : serverChannels.values()) {
            for (Subchannel subchannel : subChannelList.subChannels) {
                shutdownSubChannel(subchannel);
            }
        }
    }

    @Override
    public boolean canHandleEmptyAddressListFromNameResolution() {
        return true;
    }

    private ConnectivityStateInfo getSubChannelState(Subchannel subchannel) {
        return subchannel.getAttributes().get(Constants.STATE_INFO).get();
    }

    private void scheduleBalancingStateUpdate() {
        if (balancingStateUpdateScheduled.compareAndSet(false, true)) {
            helper.getSynchronizationContext().schedule(this::updateBalancingState,
                1,
                TimeUnit.SECONDS,
                helper.getScheduledExecutorService());
        }
    }

    private void updateBalancingState() {
        ConnectivityState newState = determineChannelState();
        if (newState != SHUTDOWN) {
            log.debug("Update balancing state to {}", newState);

            currentPicker.refresh(serverChannels);
            helper.updateBalancingState(newState, currentPicker);
            Map<String, Boolean> allServers = currentServers;
            ITenantRouter tenantRouter =
                new TenantRouter(currentServers, currentTrafficDirective, currentServerGroupTags);
            updateListener.onUpdate(new IServerSelector() {
                @Override
                public boolean exists(String serverId) {
                    return allServers.containsKey(serverId);
                }

                @Override
                public IServerGroupRouter get(String tenantId) {
                    return tenantRouter.get(tenantId);
                }

                @Override
                public String toString() {
                    return allServers.toString();
                }
            });
        }
        balancingStateUpdateScheduled.set(false);
    }

    private ConnectivityState determineChannelState() {
        // channel connectivity state aggregation rule:
        // if there is no subchannel or all subchannelRegistry are in TRANSIENT_FAILURE state, the final state is
        // TRANSIENT_FAILURE
        // if no ready subchannel, the final state is CONNECTING
        // if all subchannel shutdown, the final state is SHUTDOWN
        // otherwise state is READY
        ConnectivityState connectivityState = READY;
        if (serverChannels.isEmpty() || serverChannels.values().stream()
            .map(scList -> scList.subChannels)
            .flatMap(Collection::stream)
            .map(this::getSubChannelState)
            .allMatch(state -> state.getState() == TRANSIENT_FAILURE)) {
            connectivityState = TRANSIENT_FAILURE;
        } else {
            if (serverChannels.values().stream()
                .map(scList -> scList.subChannels)
                .flatMap(Collection::stream)
                .map(this::getSubChannelState)
                .allMatch(state -> state.getState() == SHUTDOWN)) {
                connectivityState = SHUTDOWN;
            } else if (serverChannels.values().stream()
                .map(scList -> scList.subChannels)
                .flatMap(Collection::stream)
                .map(this::getSubChannelState)
                .allMatch(state -> state.getState() != READY)) {
                connectivityState = CONNECTING;
            }
        }
        return connectivityState;
    }

    private Subchannel setupSubchannel(String serverId,
                                       EquivalentAddressGroup equivalentAddressGroup,
                                       boolean inProc) {
        final Subchannel subchannel = checkNotNull(
            helper.createSubchannel(CreateSubchannelArgs.newBuilder()
                .setAddresses(equivalentAddressGroup)
                .setAttributes(Attributes.newBuilder()
                    .set(Constants.STATE_INFO,
                        new AtomicReference<>(ConnectivityStateInfo.forNonError(IDLE)))
                    .set(IN_PROC_SERVER_ATTR_KEY, inProc)
                    .set(SERVER_ID_ATTR_KEY, serverId)
                    .build())
                .build()),
            "subchannel");
        subchannel.start(state -> handleSubchannelStateChange(subchannel, state));
        subchannel.requestConnection();
        return subchannel;
    }

    private void handleSubchannelStateChange(Subchannel subchannel, ConnectivityStateInfo state) {
        updateSubChannelState(subchannel, state);
        scheduleBalancingStateUpdate();
    }

    private void shutdownSubChannel(Subchannel subchannel) {
        log.trace("Shutdown sub-channel: {}", subchannel);
        subchannel.shutdown();
        updateSubChannelState(subchannel, ConnectivityStateInfo.forNonError(SHUTDOWN));
    }

    private void updateSubChannelState(Subchannel subchannel, ConnectivityStateInfo state) {
        log.trace("Sub-channel[{}] state change to {}", subchannel, state);
        subchannel.getAttributes().get(Constants.STATE_INFO).set(state);
        if (state.getState() == IDLE) {
            subchannel.requestConnection();
        }
    }

    private static <T> Set<T> difference(Set<T> a, Set<T> b) {
        Set<T> aCopy = new HashSet<>(a);
        aCopy.removeAll(b);
        return aCopy;
    }
}
