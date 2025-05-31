/*
 * Copyright (c) 2023. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.basecluster.memberlist;

import static com.github.benmanes.caffeine.cache.Scheduler.systemScheduler;

import org.apache.bifromq.basecluster.membership.proto.Endorse;
import org.apache.bifromq.basecluster.membership.proto.Fail;
import org.apache.bifromq.basecluster.membership.proto.HostEndpoint;
import org.apache.bifromq.basecluster.membership.proto.HostMember;
import org.apache.bifromq.basecluster.membership.proto.Join;
import org.apache.bifromq.basecluster.membership.proto.Quit;
import org.apache.bifromq.basecluster.messenger.IMessenger;
import org.apache.bifromq.basecluster.proto.ClusterMessage;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Maps;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class AutoHealer {
    private final Cache<HostEndpoint, Integer> healingMembers;
    private final Cache<HostEndpoint, Integer> quitMembers;
    private final IMessenger messenger;
    private final Scheduler scheduler;
    private final IHostMemberList memberList;
    private final IHostAddressResolver addressResolver;
    private final Duration healingTimeout;
    private final Duration healingInterval;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final Gauge healingNumGauge;
    private volatile Map<HostEndpoint, Integer> alivePeers = new HashMap<>();
    private volatile Disposable healingJob;

    public AutoHealer(IMessenger messenger,
                      Scheduler scheduler,
                      IHostMemberList memberList,
                      IHostAddressResolver addressResolver,
                      Duration healingTimeout,
                      Duration healingInterval,
                      String... tags) {
        this.messenger = messenger;
        this.scheduler = scheduler;
        this.memberList = memberList;
        this.addressResolver = addressResolver;
        this.healingTimeout = healingTimeout;
        this.healingInterval = healingInterval;
        this.healingMembers = Caffeine.newBuilder()
            .maximumSize(30)
            .expireAfterWrite(healingTimeout)
            .scheduler(systemScheduler())
            .removalListener((RemovalListener<HostEndpoint, Integer>) (key, value, cause) -> {
                if (cause.wasEvicted()) {
                    log.debug("Give up healing host[{}]", key);
                }
            })
            .build();
        this.quitMembers = Caffeine.newBuilder()
            .maximumSize(30)
            .expireAfterWrite(Duration.ofSeconds(300))
            .scheduler(systemScheduler())
            .build();
        disposables.add(messenger.receive().map(m -> m.value().message)
            .observeOn(scheduler)
            .subscribe(this::handleMessage));
        disposables.add(memberList.members()
            .observeOn(scheduler)
            .subscribe(this::syncAlivePeers));
        healingNumGauge = Gauge.builder("basecluster.heal.num", healingMembers::estimatedSize)
            .tags(tags)
            .register(Metrics.globalRegistry);
    }

    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            healingMembers.invalidateAll();
            disposables.dispose();
            if (healingJob != null) {
                healingJob.dispose();
            }
            Metrics.globalRegistry.remove(healingNumGauge);
        }
    }

    private void handleMessage(ClusterMessage message) {
        switch (message.getClusterMessageTypeCase()) {
            case JOIN -> handleJoin(message.getJoin());
            case ENDORSE -> handleEndorse(message.getEndorse());
            case QUIT -> handleQuit(message.getQuit());
            case FAIL -> handleFail(message.getFail());
        }
    }

    private void handleJoin(Join join) {
        HostMember joinMember = join.getMember();
        HostEndpoint joinEndpoint = joinMember.getEndpoint();
        Integer healingMemberIncarnation = healingMembers.getIfPresent(joinEndpoint);
        if (healingMemberIncarnation != null) {
            if (healingMemberIncarnation < joinMember.getIncarnation()) {
                // the member is alive, no need to heal it anymore
                log.debug("Member[{},{}] is reachable now, stop healing: local={}",
                    joinEndpoint, healingMemberIncarnation, memberList.local().getEndpoint());
                healingMembers.invalidate(joinEndpoint);
            }
        } else {
            // scan the healing member and invalid the members with same address
            cleanSameAddressHealingMembers(joinEndpoint);
        }
    }

    private void handleEndorse(Endorse endorse) {
        HostEndpoint endpoint = endorse.getEndpoint();
        Integer healingMemberIncarnation = healingMembers.getIfPresent(endpoint);
        if (healingMemberIncarnation != null) {
            if (healingMemberIncarnation <= endorse.getIncarnation()) {
                // the member is alive, no need to heal it anymore
                log.debug("Member[{},{}] is confirmed alive by others, stop healing: local={}",
                    endpoint, endorse.getIncarnation(), memberList.local().getEndpoint());
                healingMembers.invalidate(endpoint);
            }
        } else {
            // scan the healing member and invalid the members with same address
            cleanSameAddressHealingMembers(endpoint);
        }
    }

    private void cleanSameAddressHealingMembers(HostEndpoint endpoint) {
        // scan the healing member and invalid the members with same address
        for (HostEndpoint healingEndpoint : healingMembers.asMap().keySet()) {
            if (healingEndpoint.getAddress().equals(endpoint.getAddress())) {
                log.debug("Member[{}] has been healed, stop healing: local={}",
                    healingEndpoint, memberList.local().getEndpoint());
                healingMembers.invalidate(healingEndpoint);
            }
        }
    }

    private void handleQuit(Quit quit) {
        HostEndpoint quitEndpoint = quit.getEndpoint();
        Integer healingMemberIncarnation = healingMembers.getIfPresent(quitEndpoint);
        if (healingMemberIncarnation != null) {
            if (healingMemberIncarnation <= quit.getIncarnation()) {
                // the member has quit, no need to heal it anymore
                log.debug("Member[{},{}] has quit, stop healing: local={}",
                    quitEndpoint, quit.getIncarnation(), memberList.local().getEndpoint());
                healingMembers.invalidate(quitEndpoint);
                quitMembers.put(quitEndpoint, quit.getIncarnation());
            }
        } else {
            quitMembers.put(quitEndpoint, quit.getIncarnation());
        }
    }

    private void handleFail(Fail fail) {
        HostEndpoint failedEndpoint = fail.getEndpoint();
        // no need to heal self, memberlist will refute it
        Integer inc = alivePeers.get(failedEndpoint);
        Integer quitInc = quitMembers.getIfPresent(failedEndpoint);
        if (!failedEndpoint.equals(memberList.local().getEndpoint())
            && !memberList.isZombie(failedEndpoint)
            && inc != null
            && inc <= fail.getIncarnation()
            && (quitInc == null || quitInc < fail.getIncarnation())) {
            log.debug("Member[{},{}] has failed, add it to healing list: local={}",
                failedEndpoint, fail.getIncarnation(), memberList.local().getEndpoint());
            Map<HostEndpoint, Integer> updatedAlivePeers = Maps.newHashMap(alivePeers);
            updatedAlivePeers.remove(failedEndpoint, inc);
            alivePeers = updatedAlivePeers;
            healingMembers.put(failedEndpoint, fail.getIncarnation());
            quitMembers.invalidate(failedEndpoint);
            scheduleHealing();
        }
    }

    private void scheduleHealing() {
        if (stopped.get()) {
            return;
        }
        if (scheduled.compareAndSet(false, true)) {
            healingJob = scheduler.scheduleDirect(this::heal, healingInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void heal() {
        for (Map.Entry<HostEndpoint, Integer> entry : healingMembers.asMap().entrySet()) {
            HostEndpoint healMember = entry.getKey();
            int incarnation = entry.getValue();
            log.debug(
                "Send join message to the host[{}:{}] locating at address[{}:{}] for healing the connection: incarnation={}",
                healMember.getId(), healMember.getPid(), healMember.getAddress(), healMember.getPort(), incarnation);
            messenger.send(ClusterMessage.newBuilder()
                .setJoin(Join.newBuilder()
                    .setMember(memberList.local())
                    .setExpectedHost(healMember)
                    .build())
                .build(), addressResolver.resolve(healMember), true);
        }
        // run a compaction so that expired entries could be cleanup as soon as possible
        healingMembers.cleanUp();
        scheduled.set(false);
        if (!healingMembers.asMap().isEmpty()) {
            scheduleHealing();
        }
    }

    private void syncAlivePeers(Map<HostEndpoint, Integer> members) {
        Map<HostEndpoint, Integer> newAlivePeers =
            Maps.filterKeys(members, k -> !k.equals(memberList.local().getEndpoint()));
        Maps.difference(newAlivePeers, alivePeers).entriesOnlyOnLeft()
            .forEach((k, v) -> cleanSameAddressHealingMembers(k));
        alivePeers = newAlivePeers;
    }
}
