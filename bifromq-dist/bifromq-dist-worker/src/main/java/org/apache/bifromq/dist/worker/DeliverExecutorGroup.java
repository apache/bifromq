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

package org.apache.bifromq.dist.worker;

import static org.apache.bifromq.metrics.TenantMetric.MqttPersistentFanOutBytes;
import static org.apache.bifromq.plugin.eventcollector.ThreadLocalEventPool.getLocal;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalPersistentFanOutBytesPerSeconds;
import static org.apache.bifromq.plugin.resourcethrottler.TenantResourceType.TotalTransientFanOutBytesPerSeconds;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxGroupFanout;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxPersistentFanout;
import static org.apache.bifromq.plugin.settingprovider.Setting.MaxPersistentFanoutBytes;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.base.Charsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bifromq.base.util.RendezvousHash;
import org.apache.bifromq.deliverer.IMessageDeliverer;
import org.apache.bifromq.deliverer.TopicMessagePackHolder;
import org.apache.bifromq.dist.worker.schema.GroupMatching;
import org.apache.bifromq.dist.worker.schema.Matching;
import org.apache.bifromq.dist.worker.schema.NormalMatching;
import org.apache.bifromq.metrics.ITenantMeter;
import org.apache.bifromq.plugin.eventcollector.IEventCollector;
import org.apache.bifromq.plugin.eventcollector.OutOfTenantResource;
import org.apache.bifromq.plugin.eventcollector.distservice.GroupFanoutThrottled;
import org.apache.bifromq.plugin.eventcollector.distservice.PersistentFanoutBytesThrottled;
import org.apache.bifromq.plugin.eventcollector.distservice.PersistentFanoutThrottled;
import org.apache.bifromq.plugin.resourcethrottler.IResourceThrottler;
import org.apache.bifromq.plugin.resourcethrottler.TenantResourceType;
import org.apache.bifromq.plugin.settingprovider.ISettingProvider;
import org.apache.bifromq.sysprops.props.DistTopicMatchExpirySeconds;
import org.apache.bifromq.type.ClientInfo;
import org.apache.bifromq.type.RouteMatcher;
import org.apache.bifromq.type.TopicMessagePack;
import org.apache.bifromq.util.SizeUtil;

@Slf4j
class DeliverExecutorGroup implements IDeliverExecutorGroup {
    // OuterCacheKey: OrderedSharedMatchingKey(<tenantId>, <mqttTopicFilter>)
    // InnerCacheKey: ClientInfo(<tenantId>, <type>, <metadata>)
    private final LoadingCache<OrderedSharedMatchingKey, Cache<ClientInfo, NormalMatching>> orderedSharedMatching;
    private final int inlineFanOutThreshold;
    private final IEventCollector eventCollector;
    private final IResourceThrottler resourceThrottler;
    private final ISettingProvider settingProvider;
    private final DeliverExecutor[] fanoutExecutors;

    DeliverExecutorGroup(IMessageDeliverer deliverer,
                         IEventCollector eventCollector,
                         IResourceThrottler resourceThrottler,
                         ISettingProvider settingProvider,
                         int fanoutParallelism,
                         int inlineFanOutThreshold) {
        this.eventCollector = eventCollector;
        this.resourceThrottler = resourceThrottler;
        this.settingProvider = settingProvider;
        this.inlineFanOutThreshold = inlineFanOutThreshold;
        int expirySec = DistTopicMatchExpirySeconds.INSTANCE.get();
        orderedSharedMatching = Caffeine.newBuilder()
            .expireAfterAccess(expirySec * 2L, TimeUnit.SECONDS)
            .scheduler(Scheduler.systemScheduler())
            .removalListener((RemovalListener<OrderedSharedMatchingKey, Cache<ClientInfo, NormalMatching>>)
                (key, value, cause) -> {
                    if (value != null) {
                        value.invalidateAll();
                    }
                })
            .build(k -> Caffeine.newBuilder()
                .expireAfterAccess(expirySec, TimeUnit.SECONDS)
                .build());
        fanoutExecutors = new DeliverExecutor[fanoutParallelism];
        for (int i = 0; i < fanoutParallelism; i++) {
            fanoutExecutors[i] = new DeliverExecutor(i, deliverer, eventCollector);
        }
    }

    @Override
    public void shutdown() {
        for (DeliverExecutor fanoutExecutor : fanoutExecutors) {
            fanoutExecutor.shutdown();
        }
        orderedSharedMatching.invalidateAll();
    }

    @Override
    public void submit(String tenantId, Set<Matching> routes, TopicMessagePack msgPack) {
        int msgPackSize = SizeUtil.estSizeOf(msgPack);
        TopicMessagePackHolder messagePackHolder = TopicMessagePackHolder.hold(msgPack);
        if (routes.size() == 1) {
            Matching matching = routes.iterator().next();
            switch (matching.type()) {
                case Normal -> {
                    NormalMatching normalMatching = (NormalMatching) matching;
                    send(normalMatching, messagePackHolder, true);
                    if (normalMatching.subBrokerId() == 1) {
                        ITenantMeter.get(matching.tenantId()).recordSummary(MqttPersistentFanOutBytes, msgPackSize);
                    }
                }
                case Group -> send((GroupMatching) matching, messagePackHolder, true);
                default -> {
                    // never happen
                }
            }
        } else if (routes.size() > 1) {
            int maxPFanoutCount = settingProvider.provide(MaxPersistentFanout, tenantId);
            long maxPFanoutBytes = settingProvider.provide(MaxPersistentFanoutBytes, tenantId);
            int maxGFanoutCount = settingProvider.provide(MaxGroupFanout, tenantId);
            boolean inline = routes.size() < inlineFanOutThreshold;
            boolean hasTransientFanoutBandwidth =
                resourceThrottler.hasResource(tenantId, TotalTransientFanOutBytesPerSeconds);
            boolean isTransientFanoutThrottled = false;
            boolean hasPersistentFanoutBandwidth =
                resourceThrottler.hasResource(tenantId, TenantResourceType.TotalPersistentFanOutBytesPerSeconds);
            boolean isPersistentFanoutThrottled = false;
            boolean isGroupFanoutThrottled = false;
            // we meter persistent fanout bytes here, since for transient fanout is actually happened in the broker
            int persistentFanoutCount = 0;
            long persistentFanoutBytes = 0;
            int groupFanoutCount = 0;
            for (Matching matching : routes) {
                switch (matching.type()) {
                    case Normal -> {
                        NormalMatching normalMatching = (NormalMatching) matching;
                        if (normalMatching.subBrokerId() == 1) {
                            // persistent fanout
                            if (persistentFanoutCount < maxPFanoutCount && persistentFanoutBytes < maxPFanoutBytes) {
                                if (hasPersistentFanoutBandwidth) {
                                    persistentFanoutCount++;
                                    persistentFanoutBytes += msgPackSize;
                                    send(normalMatching, messagePackHolder, inline);
                                } else {
                                    if (!isPersistentFanoutThrottled) {
                                        isPersistentFanoutThrottled = true;
                                        for (TopicMessagePack.PublisherPack publisherPack : msgPack.getMessageList()) {
                                            eventCollector.report(getLocal(OutOfTenantResource.class)
                                                .reason(TotalPersistentFanOutBytesPerSeconds.name())
                                                .clientInfo(publisherPack.getPublisher())
                                            );
                                        }
                                    }
                                }
                            } else {
                                if (!isPersistentFanoutThrottled) {
                                    isPersistentFanoutThrottled = true;
                                    // persistent fanout throttled
                                    if (persistentFanoutCount >= maxPFanoutCount) {
                                        eventCollector.report(getLocal(PersistentFanoutThrottled.class)
                                            .tenantId(tenantId)
                                            .topic(msgPack.getTopic())
                                            .maxCount(maxPFanoutCount)
                                        );
                                    }
                                    if (persistentFanoutBytes >= maxPFanoutBytes) {
                                        eventCollector.report(getLocal(PersistentFanoutBytesThrottled.class)
                                            .tenantId(tenantId)
                                            .topic(msgPack.getTopic())
                                            .maxBytes(maxPFanoutBytes)
                                        );
                                    }
                                }
                            }
                        } else {
                            // transient fanout
                            if (hasTransientFanoutBandwidth) {
                                send(normalMatching, messagePackHolder, inline);
                            } else {
                                if (!isTransientFanoutThrottled) {
                                    isTransientFanoutThrottled = true;
                                    for (TopicMessagePack.PublisherPack publisherPack : msgPack.getMessageList()) {
                                        eventCollector.report(getLocal(OutOfTenantResource.class)
                                            .reason(TotalTransientFanOutBytesPerSeconds.name())
                                            .clientInfo(publisherPack.getPublisher())
                                        );
                                    }
                                }
                            }
                        }
                    }
                    case Group -> {
                        if (groupFanoutCount < maxGFanoutCount) {
                            groupFanoutCount++;
                            send((GroupMatching) matching, messagePackHolder, inline);
                        } else {
                            if (!isGroupFanoutThrottled) {
                                isGroupFanoutThrottled = true;
                                // group fanout throttled
                                eventCollector.report(getLocal(GroupFanoutThrottled.class)
                                    .tenantId(tenantId)
                                    .topic(msgPack.getTopic())
                                    .maxCount(maxGFanoutCount)
                                );
                            }
                        }
                    }
                    default -> {
                        // never happen
                    }
                }
                if (isPersistentFanoutThrottled && isTransientFanoutThrottled && isGroupFanoutThrottled) {
                    break;
                }
            }
            ITenantMeter.get(tenantId).recordSummary(MqttPersistentFanOutBytes, persistentFanoutBytes);
        }
    }

    @Override
    public void refreshOrderedShareSubRoutes(String tenantId, RouteMatcher routeMatcher) {
        if (log.isDebugEnabled()) {
            log.debug("Refresh ordered shared sub routes: tenantId={}, sharedTopicFilter={}", tenantId, routeMatcher);
        }
        orderedSharedMatching.invalidate(
            new OrderedSharedMatchingKey(tenantId, routeMatcher.getMqttTopicFilter()));
    }

    private void send(GroupMatching groupMatching, TopicMessagePackHolder msgPackHolder, boolean inline) {
        if (!groupMatching.ordered) {
            // pick one route randomly
            send(groupMatching.receiverList.get(
                ThreadLocalRandom.current().nextInt(groupMatching.receiverList.size())), msgPackHolder, inline);
        } else {
            // ordered shared subscription
            Map<NormalMatching, TopicMessagePack.Builder> orderedRoutes = new HashMap<>();
            for (TopicMessagePack.PublisherPack publisherPack : msgPackHolder.messagePack.getMessageList()) {
                ClientInfo sender = publisherPack.getPublisher();
                NormalMatching matchedInbox = orderedSharedMatching
                    .get(new OrderedSharedMatchingKey(groupMatching.tenantId(),
                        groupMatching.matcher.getMqttTopicFilter()))
                    .get(sender, senderInfo -> {
                        RendezvousHash<ClientInfo, NormalMatching> hash =
                            RendezvousHash.<ClientInfo, NormalMatching>builder()
                                .keyFunnel((from, into) -> into.putInt(from.hashCode()))
                                .nodeFunnel((from, into) -> into.putString(from.receiverUrl(), Charsets.UTF_8))
                                .nodes(groupMatching.receiverList)
                                .build();
                        NormalMatching matchRecord = hash.get(senderInfo);
                        log.debug("Ordered shared matching: sender={}: topicFilter={}, receiverId={}, subBroker={}",
                            senderInfo,
                            matchRecord.mqttTopicFilter(),
                            matchRecord.matchInfo().getReceiverId(),
                            matchRecord.subBrokerId());
                        return matchRecord;
                    });
                // ordered share sub
                orderedRoutes.computeIfAbsent(matchedInbox, k -> TopicMessagePack.newBuilder())
                    .setTopic(msgPackHolder.messagePack.getTopic())
                    .addMessage(publisherPack);
            }
            orderedRoutes.forEach((route, msgPackBuilder) ->
                send(route, TopicMessagePackHolder.hold(msgPackBuilder.build()), inline));
        }
    }

    private void send(NormalMatching route, TopicMessagePackHolder msgPackHolder, boolean inline) {
        int idx = route.hashCode() % fanoutExecutors.length;
        if (idx < 0) {
            idx += fanoutExecutors.length;
        }
        fanoutExecutors[idx].submit(route, msgPackHolder, inline);
    }

    private record OrderedSharedMatchingKey(String tenantId, String mqttTopicFilter) {
    }
}
